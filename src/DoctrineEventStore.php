<?php
declare(strict_types=1);
namespace Neos\EventStore\DoctrineAdapter;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\Exception as DriverException;
use Doctrine\DBAL\Exception as DbalException;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\Result;
use Doctrine\DBAL\Schema\Comparator;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Schema\SchemaConfig;
use Doctrine\DBAL\Types\Types;
use Neos\EventStore\EventStoreInterface;
use Neos\EventStore\Exception\ConcurrencyException;
use Neos\EventStore\Helper\BatchEventStreamInterface;
use Neos\EventStore\Model\EventStore\CommitResult;
use Neos\EventStore\Model\EventStream\EventStreamInterface;
use Neos\EventStore\Model\EventStream\ExpectedVersion;
use Neos\EventStore\Model\EventStream\MaybeVersion;
use Neos\EventStore\Model\Event\SequenceNumber;
use Neos\EventStore\Model\EventStore\SetupResult;
use Neos\EventStore\Model\EventStore\Status;
use Neos\EventStore\Model\Event\StreamName;
use Neos\EventStore\Model\Event\Version;
use Neos\EventStore\Model\EventStream\VirtualStreamName;
use Neos\EventStore\Model\EventStream\VirtualStreamType;
use Neos\EventStore\Model\Event;
use Neos\EventStore\Model\Events;
use Neos\EventStore\ProvidesStatusInterface;
use Neos\EventStore\ProvidesSetupInterface;

final class DoctrineEventStore implements EventStoreInterface, ProvidesSetupInterface, ProvidesStatusInterface
{
    /**
     * @readonly
     */
    private Connection $connection;
    /**
     * @readonly
     */
    private string $eventTableName;
    public function __construct(Connection $connection, string $eventTableName)
    {
        $this->connection = $connection;
        $this->eventTableName = $eventTableName;
    }
    /**
     * @param \Neos\EventStore\Model\EventStream\VirtualStreamName|\Neos\EventStore\Model\Event\StreamName $streamName
     */
    public function load($streamName): EventStreamInterface
    {
        $this->reconnectDatabaseConnection();
        $queryBuilder = $this->connection->createQueryBuilder()
            ->select('*')
            ->from($this->eventTableName)
            ->orderBy('sequencenumber', 'ASC');

        switch (get_class($streamName)) {
            case StreamName::class:
                $queryBuilder = $queryBuilder->andWhere('stream = :streamName')->setParameter('streamName', $streamName->value);
                break;
            case VirtualStreamName::class:
                switch ($streamName->type) {
                    case VirtualStreamType::ALL:
                        $queryBuilder = $queryBuilder;
                        break;
                    case VirtualStreamType::CATEGORY:
                        $queryBuilder = $queryBuilder->andWhere('stream LIKE :streamNamePrefix')->setParameter('streamNamePrefix', $streamName->value . '%');
                        break;
                    case VirtualStreamType::CORRELATION_ID:
                        $queryBuilder = $queryBuilder->andWhere('correlationIdentifier LIKE :correlationId')->setParameter('correlationId', $streamName->value);
                        break;
                }
                break;
            default:
                $queryBuilder = $queryBuilder;
                break;
        }
        return BatchEventStreamInterface::create(DoctrineEventStream::create($queryBuilder), 100);
    }

    public function commit(StreamName $streamName, Events $events, ExpectedVersion $expectedVersion): CommitResult
    {
        # Exponential backoff: initial interval = 5ms and 8 retry attempts = max 1275ms (= 1,275 seconds)
        # @see http://backoffcalculator.com/?attempts=8&rate=2&interval=5
        $retryWaitInterval = 0.005;
        $maxRetryAttempts = 8;
        $retryAttempt = 0;
        while (true) {
            $this->reconnectDatabaseConnection();
            if ($this->connection->getTransactionNestingLevel() > 0) {
                throw new \RuntimeException('A transaction is active already, can\'t commit events!', 1547829131);
            }
            $this->connection->beginTransaction();
            try {
                $maybeVersion = $this->getStreamVersion($streamName);
                $expectedVersion->verifyVersion($maybeVersion);
                $version = $maybeVersion->isNothing() ? Version::first() : $maybeVersion->unwrap()->next();
                $now = new \DateTimeImmutable('now');
                foreach ($events as $event) {
                    $this->commitEvent($streamName, $event, $version, $now);
                    $version = $version->next();
                }
                $lastInsertId = $this->connection->lastInsertId();
                if (!is_numeric($lastInsertId)) {
                    throw new \RuntimeException(sprintf('Expected last insert id to be numeric, but it is: %s', get_debug_type($lastInsertId)), 1651749706);
                }
                $this->connection->commit();
                return new CommitResult($version, SequenceNumber::fromInteger((int)$lastInsertId));
            } catch (UniqueConstraintViolationException $exception) {
                if ($retryAttempt >= $maxRetryAttempts) {
                    $this->connection->rollBack();
                    throw new ConcurrencyException(sprintf('Failed after %d retry attempts', $retryAttempt), 1573817175, $exception);
                }
                usleep((int)($retryWaitInterval * 1E6));
                $retryAttempt++;
                $retryWaitInterval *= 2;
                $this->connection->rollBack();
                continue;
            } catch (DbalException | ConcurrencyException | \JsonException $exception) {
                $this->connection->rollBack();
                throw $exception;
            }
        }
    }

    public function deleteStream(StreamName $streamName): void
    {
        $this->connection->delete($this->eventTableName, [
            'stream' => $streamName->value
        ]);
    }

    public function status(): Status
    {
        return Status::error('not implemented');
    }

    public function setup(): SetupResult
    {
        $schemaManager = $this->connection->getSchemaManager();
        assert($schemaManager !== null);
        $fromSchema = $schemaManager->createSchema();
        $schemaDiff = (new Comparator())->compare($fromSchema, $this->createEventStoreSchema());

        $statements = $schemaDiff->toSaveSql($this->connection->getDatabasePlatform());
        if ($statements === []) {
            return SetupResult::success('Table schema is up to date, no migration required');
        }

        foreach ($statements as $statement) {
            $this->connection->executeStatement($statement);
        }
        return SetupResult::success('Event store table created/updated successfully');
    }


    // ----------------------------------

    /**
     * Creates the Doctrine schema to be compared with the current db schema for migration
     *
     * @return Schema
     */
    private function createEventStoreSchema(): Schema
    {
        $schemaConfiguration = new SchemaConfig();
        $connectionParameters = $this->connection->getParams();
        if (isset($connectionParameters['defaultTableOptions'])) {
            $schemaConfiguration->setDefaultTableOptions($connectionParameters['defaultTableOptions']);
        }
        $schema = new Schema([], [], $schemaConfiguration);
        $table = $schema->createTable($this->eventTableName);

        // The monotonic sequence number
        $table->addColumn('sequencenumber', Types::INTEGER, ['autoincrement' => true]);
        // The stream name, usually in the format "<BoundedContext>:<StreamName>"
        $table->addColumn('stream', Types::STRING, ['length' => 255]);
        // Version of the event in the respective stream
        $table->addColumn('version', Types::BIGINT, ['unsigned' => true]);
        // The event type in the format "<BoundedContext>:<EventType>"
        $table->addColumn('type', Types::STRING, ['length' => 255]);
        // The event payload as JSON
        $table->addColumn('payload', Types::TEXT);
        // The event metadata as JSON
        $table->addColumn('metadata', Types::TEXT);
        // The unique event id, usually a UUID
        $table->addColumn('id', Types::STRING, ['length' => 255]);
        // An optional correlation id, usually a UUID
        $table->addColumn('correlationidentifier', Types::STRING, ['length' => 255, 'notnull' => false]);
        // An optional causation id, usually a UUID
        $table->addColumn('causationidentifier', Types::STRING, ['length' => 255, 'notnull' => false]);
        // Timestamp of the the event publishing
        $table->addColumn('recordedat', Types::DATETIME_IMMUTABLE);

        $table->setPrimaryKey(['sequencenumber']);
        $table->addUniqueIndex(['id'], 'id_uniq');
        $table->addUniqueIndex(['stream', 'version'], 'stream_version_uniq');
        $table->addIndex(['correlationidentifier']);

        return $schema;
    }

    /**
     * @throws DriverException
     * @throws DbalException
     */
    private function getStreamVersion(StreamName $streamName): MaybeVersion
    {
        $result = $this->connection->createQueryBuilder()
            ->select('MAX(version)')
            ->from($this->eventTableName)
            ->where('stream = :streamName')
            ->setParameter('streamName', $streamName->value)
            ->execute();
        if (!$result instanceof Result) {
            throw new \RuntimeException(sprintf('Failed to determine stream version of stream "%s"', $streamName->value), 1651153859);
        }
        $version = $result->fetchOne();
        return MaybeVersion::fromVersionOrNull(is_numeric($version) ? Version::fromInteger((int)$version) : null);
    }

    /**
     * @throws DbalException | UniqueConstraintViolationException| \JsonException
     */
    private function commitEvent(StreamName $streamName, Event $event, Version $version, \DateTimeImmutable $now): void
    {
        $this->connection->insert(
            $this->eventTableName,
            [
                'id' => $event->id->value,
                'stream' => $streamName->value,
                'version' => $version->value,
                'type' => $event->type->value,
                'payload' => $event->data->value,
                'metadata' => $event->metadata->toJson(),
                'correlationidentifier' => $event->metadata->get('correlationIdentifier'),
                'causationidentifier' => $event->metadata->get('causationIdentifier'),
                'recordedat' => $now,
            ],
            [
                'version' => Types::INTEGER,
                'recordedat' => Types::DATETIME_IMMUTABLE,
            ]
        );
    }

    private function reconnectDatabaseConnection(): void
    {
        try {
            $this->connection->fetchOne('SELECT 1');
        } catch (\Exception $_) {
            $this->connection->close();
            $this->connection->connect();
        }
    }
}
