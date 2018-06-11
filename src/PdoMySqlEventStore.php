<?php

/*
 * This file is part of the broadway/broadway package.
 *
 * (c) Qandidate.com <opensource@qandidate.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Broadway\EventStore\Dbal;

use Broadway\Domain\DateTime;
use Broadway\Domain\DomainEventStream;
use Broadway\Domain\DomainMessage;
use Broadway\EventStore\EventStore;
use Broadway\EventStore\EventStreamNotFoundException;
use Broadway\EventStore\EventVisitor;
use Broadway\EventStore\Exception\DuplicatePlayheadException;
use Broadway\EventStore\Exception\InvalidIdentifierException;
use Broadway\EventStore\Management\Criteria;
use Broadway\EventStore\Management\CriteriaNotSupportedException;
use Broadway\EventStore\Management\EventStoreManagement;
use Broadway\Serializer\Serializer;
use Broadway\UuidGenerator\Converter\BinaryUuidConverterInterface;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\Exception\UniqueConstraintViolationException;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Version;

/**
 * Event store using Doctrine DBAL with the PDOMySql driver using the capabilities of
 * MySQL >= 5.7.9 like the JSON Data Type and Virtual Columns
 */
class PdoMySqlEventStore extends DBALEventStore
{
    public function __construct(
        Connection $connection,
        Serializer $payloadSerializer,
        Serializer $metadataSerializer,
        string $tableName
    ) {

        if (! $connection->getDatabasePlatform()->hasNativeJsonType()) {
            throw new \InvalidArgumentException('The JSON Data Type is not available');
        }

        parent::__construct($connection, $payloadSerializer, $metadataSerializer, $tableName, false);
    }

    /**
     * {@inheritdoc}
     */
    public function configureTable(Schema $schema = null)
    {
        $schema = $schema ?: new Schema();

        $table = $schema->createTable($this->tableName);

        $table->addColumn('uuid', 'guid', ['columnDefinition' => 'char(36) AS (JSON_EXTRACT(\'payload\', \'$.id\'))']);
        $table->addColumn('playhead', 'integer', ['columnDefinition' => 'int(10) unsigned AS (JSON_EXTRACT(\'payload\', \'$.payload\'))']);
        $table->addColumn('payload', 'json');
        $table->addColumn('metadata', 'json');
        $table->addColumn('recorded_on', 'string', ['length' => 32]);
        $table->addColumn('type', 'string', ['length' => 255]);

        $table->addUniqueIndex(['uuid', 'playhead']);

        return $table;
    }

    /**
     * {@inheritdoc}
     */
    protected function insertMessage(Connection $connection, DomainMessage $domainMessage)
    {
        $data = [
            'metadata'    => json_encode($this->metadataSerializer->serialize($domainMessage->getMetadata())),
            'payload'     => json_encode($this->payloadSerializer->serialize($domainMessage->getPayload())),
            'recorded_on' => $domainMessage->getRecordedOn()->toString(),
            'type'        => $domainMessage->getType(),
        ];

        $connection->insert($this->tableName, $data);
    }
}
