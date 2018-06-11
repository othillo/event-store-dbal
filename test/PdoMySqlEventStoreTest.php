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

use Broadway\Serializer\SimpleInterfaceSerializer;
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\AbstractSchemaManager;

/**
 * @requires extension pdo_mysql
 *
 * @group functional
 */
class PdoMySqlEventStoreTest extends DBALEventStoreTest
{
    /**
     * @var AbstractSchemaManager
     */
    private $schemaManager;

    /**
     * {@inheritdoc}
     */
    protected function setUp()
    {
        $connection = DriverManager::getConnection([
            'driver' => 'pdo_mysql',
            'host' => '127.0.0.1',
            'user' => 'root',
            'password' => 'my-secret-pw',
            'dbname' => 'broadway',
        ]);

        $this->eventStore = new PdoMySqlEventStore(
            $connection,
            new SimpleInterfaceSerializer(),
            new SimpleInterfaceSerializer(),
            'events'
        );

        $this->schemaManager = $connection->getSchemaManager();;
        if ($table = $this->eventStore->configureSchema($this->schemaManager->createSchema())) {
            $this->schemaManager->dropAndCreateTable($table);
        }

        //var_dump($connection->query('SHOW CREATE TABLE events;')->fetchAll());
    }

    /**
     * {@inheritdoc}
     */
    protected function tearDown()
    {
        //$this->schemaManager->dropTable('events');
    }
}
