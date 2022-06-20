<?php
declare(strict_types=1);
namespace Neos\EventStore\DoctrineAdapter\Tests;

use Doctrine\DBAL\Connection;
use Neos\EventStore\DoctrineAdapter\DoctrineEventStore;
use Neos\EventStore\EventStoreInterface;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;

final class DoctrineEventStoreTest extends TestCase
{

    /**
     * @var Connection|MockObject
     */
    private Connection $mockConnection;

    private DoctrineEventStore $eventStore;

    public function setUp(): void
    {
        $this->mockConnection = $this->getMockBuilder(Connection::class)->disableOriginalConstructor()->getMock();
        $this->eventStore = new DoctrineEventStore($this->mockConnection, 'table_name');
    }

    // TODO write tests :)

    public function test(): void
    {
        self::assertInstanceOf(EventStoreInterface::class, $this->eventStore);
    }

}