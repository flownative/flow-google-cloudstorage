<?php
declare(strict_types=1);

namespace Flownative\Google\CloudStorage\Command;

/*
 * This file is part of the Flownative.Google.CloudStorage package.
 *
 * (c) Robert Lemke, Flownative GmbH - www.flownative.com
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Types\Types;
use Doctrine\ORM\EntityManagerInterface;
use Flownative\Google\CloudStorage\GcsStorage;
use Flownative\Google\CloudStorage\GcsTarget;
use Flownative\Google\CloudStorage\StorageFactory;
use Google\Cloud\Core\Exception\NotFoundException;
use Google\Cloud\Core\Exception\ServiceException;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Cli\CommandController;
use Neos\Flow\ResourceManagement\ResourceManager;
use Neos\Flow\ResourceManagement\Storage\StorageObject;
use Symfony\Component\Console\Formatter\OutputFormatterStyle;

/**
 * Google Cloud Storage command controller
 *
 * @Flow\Scope("singleton")
 */
final class GcsCommandController extends CommandController
{
    private const TEMPORARY_TABLE_NAME = 'flownative_google_cloudstorage_temp';

    /**
     * @Flow\Inject
     * @var StorageFactory
     */
    protected $storageFactory;

    /**
     * @Flow\Inject
     * @var ResourceManager
     */
    protected $resourceManager;

    public function initializeObject(): void
    {
        $this->output->getOutput()->getFormatter()->setStyle('hint', new OutputFormatterStyle('yellow'));
    }

    /**
     * Checks the connection
     *
     * This command checks if the configured credentials and connectivity allows for connecting with the Google API.
     *
     * @param string $bucket The bucket which is used for trying to upload and retrieve some test data
     * @return void
     */
    public function connectCommand(string $bucket): void
    {
        try {
            $storageClient = $this->storageFactory->create();
        } catch (\Exception $e) {
            $this->outputLine('<error>%s</error>', [$e->getMessage()]);
            exit(1);
        }

        $bucketName = $bucket;
        $bucket = $storageClient->bucket($bucketName);

        $this->outputLine('Writing test object into bucket (%s) ...', [$bucketName]);
        $bucket->upload(
            'test',
            [
                'name' => 'Flownative.Google.CloudStorage.ConnectionTest.txt',
                'metadata' => [
                    'test' => true
                ]
            ]
        );

        $this->outputLine('Retrieving test object from bucket ...');
        $this->outputLine('<em>' . $bucket->object('Flownative.Google.CloudStorage.ConnectionTest.txt')->downloadAsString() . '</em>');

        $this->outputLine('Deleting test object from bucket ...');
        $bucket->object('Flownative.Google.CloudStorage.ConnectionTest.txt')->delete();

        $this->outputLine('OK');
    }

    /**
     * Republish a collection
     *
     * This command forces publishing resources of the given collection by copying resources from the respective storage
     * to target bucket.
     *
     * @param string $collection Name of the collection to publish
     */
    public function republishCommand(string $collection = 'persistent'): void
    {
        $collectionName = $collection;
        $collection = $this->resourceManager->getCollection($collectionName);
        if (!$collection) {
            $this->outputLine('<error>The collection %s does not exist.</error>', [$collectionName]);
            exit(1);
        }

        $target = $collection->getTarget();
        if (!$target instanceof GcsTarget) {
            $this->outputLine('<error>The target defined in collection %s is not a Google Cloud Storage target.</error>', [$collectionName]);
            exit(1);
        }

        $this->outputLine('Republishing collection ...');
        $this->output->progressStart();
        try {
            foreach ($collection->getObjects() as $object) {
                /** @var StorageObject $object */
                $resource = $this->resourceManager->getResourceBySha1($object->getSha1());
                if ($resource) {
                    $target->publishResource($resource, $collection);
                }
                $this->output->progressAdvance();
            }
        } catch (\Exception $e) {
            $this->outputLine('<error>Publishing failed</error>');
            $this->outputLine($e->getMessage());
            $this->outputLine(get_class($e));
            exit(2);
        }
        $this->output->progressFinish();
        $this->outputLine();
    }

    /**
     * Update resource metadata
     *
     * This command iterates through all known resources of a collection and sets the metadata in the configured target.
     * The resource must exist in the target, but metadata like "content-type" will be update.
     *
     * This command can be used for migrating from a two-bucket to a one-bucket setup, where storage and target are using
     * the same bucket.
     *
     * The resources are processed in alphabetical order of their SHA1 content hashes. That allows you to resume updates
     * at a specific resource (using the --startSha1 option) in case a large update was interrupted.
     *
     * @param string $collection Name of the collection to publish
     * @param string|null $startSha1 If specified, updates are starting at this SHA1 in alphabetical order
     * @throws \Doctrine\DBAL\DBALException
     */
    public function updateResourceMetadataCommand(string $collection = 'persistent', ?string $startSha1 = null): void
    {
        $collectionName = $collection;
        $collection = $this->resourceManager->getCollection($collection);
        if (!$collection) {
            $this->outputLine('<error>The collection %s does not exist.</error>', [$collectionName]);
            exit(1);
        }

        $target = $collection->getTarget();
        if (!$target instanceof GcsTarget) {
            $this->outputLine('<error>The target defined in collection %s is not a Google Cloud Storage target.</error>', [$collectionName]);
            exit(1);
        }

        $this->outputLine();
        $this->outputLine('Updating metadata for resources in bucket %s ...', [$target->getBucketName()]);
        $this->outputLine();

        try {
            $storageClient = $this->storageFactory->create();
        } catch (\Exception $e) {
            $this->outputLine('<error>%s</error>', [$e->getMessage()]);
            exit(1);
        }

        if ($this->objectManager->isRegistered(EntityManagerInterface::class)) {
            $entityManager = $this->objectManager->get(EntityManagerInterface::class);
        } else {
            $entityManager = $this->objectManager->get(\Doctrine\Common\Persistence\ObjectManager::class);
        }

        $targetBucket = $storageClient->bucket($target->getBucketName());
        $targetKeyPrefix = $target->getKeyPrefix();

        if ($startSha1 === null) {
            $queryResult = $entityManager->getConnection()->executeQuery(
                'SELECT sha1, filename, mediatype FROM neos_flow_resourcemanagement_persistentresource AS r WHERE collectionname = :collectionName ORDER BY sha1',
                ['collectionName' => $collectionName]
            );
        } else {
            $queryResult = $entityManager->getConnection()->executeQuery(
                'SELECT sha1, filename, mediatype FROM neos_flow_resourcemanagement_persistentresource AS r WHERE collectionname = :collectionName AND sha1 > :startSha1 ORDER BY sha1',
                [
                    'collectionName' => $collectionName,
                    'startSha1' => $startSha1
                ]
            );
        }

        try {
            $previousSha1 = null;
            while ($resourceRecord = $queryResult->fetchAssociative()) {
                if ($resourceRecord['sha1'] === $previousSha1) {
                    continue;
                }
                $previousSha1 = $resourceRecord['sha1'];

                try {
                    $object = $targetBucket->object($targetKeyPrefix . $resourceRecord['sha1']);
                    $object->update(['contentType' => $resourceRecord['mediatype']]);
                    $this->outputLine('   ✅  %s %s ', [$resourceRecord['sha1'], $resourceRecord['filename']]);
                } catch (ServiceException|NotFoundException $exception) {
                    $this->outputLine('   ❌  <error>%s %s</error>', [$resourceRecord['sha1'], $resourceRecord['filename']]);
                }
            }
        } catch (\Exception $e) {
            $this->outputLine('<error>Publishing failed</error>');
            $this->outputLine($e->getMessage());
            exit(2);
        }
        $this->outputLine();
    }

    /**
     * Clean obsolete objects
     *
     * This command iterates through all objects / files in the Google Cloud Storage bucket
     * which is configured as a storage for the specified Flow resource collection. It then
     * checks if a corresponding Persistent Resource exists in Flow. If no such resource exists in
     * the database, this command can delete the object from the Google Cloud Storage bucket.
     *
     * This command will ask for confirmation before deleting anything and can only be used
     * interactively.
     *
     * If the option --export-to-file is specified, this command exports a list of SHA1 hashes
     * of those objects which *would* be deleted. No object will be deleted if the --export-to-file
     * is specified.
     *
     * @param string $exportToFile Path and filename of a file to write to. If specified, this command will not delete obsolete objects, but write a list of SHA1 hashes which would be deleted to this file
     * @param string $collection Name of the Flow resource collection to consider. If not specified, "persistent" will be used.
     * @return void
     * @throws
     */
    public function cleanCommand(string $exportToFile = '', string $collection = 'persistent'): void
    {
        $storage = $this->getStorageFromCollection($collection);
        $connection = $this->getDatabaseConnection();
        $this->createTemporaryTable($connection);

        $this->outputLine('Preparing to clean up obsolete objects in Google Cloud Storage');
        $this->outputLine('Using storage bucket <b>%s</b>', [$storage->getBucketName()]);

        $storageObjectsResourceHashesCount = $connection->executeQuery('SELECT COUNT(*) FROM flownative_google_cloudstorage_temp')->fetchOne();

        if ($storageObjectsResourceHashesCount > 0) {
            $this->outputLine();
            $this->outputLine('<hint>Found analysis data from a previous run</hint>');
            if ($this->output->askConfirmation('Proceed with existing data? ', true)) {
                $this->outputLine('→ Using results from previous run');
            } else {
                $this->outputLine('<success>Removing analysis data from previous run</success>');
                $connection->executeQuery('TRUNCATE TABLE ' . self::TEMPORARY_TABLE_NAME);
                $storageObjectsResourceHashesCount = 0;
            }
        }

        if ($storageObjectsResourceHashesCount === 0) {
            $storageObjectsResourceHashesCount = $this->retrieveStorageObjectsResourcesHashes($storage, $connection);
        }

        $this->outputLine('The bucket contains %s storage objects', [$storageObjectsResourceHashesCount]);

        $this->outputLine();
        $this->outputLine('<success>Matching object hashes with resources in the database ...</success>', [$storage->getBucketName()]);

        $query = <<<SQL
            SELECT `sha1`
            FROM flownative_google_cloudstorage_temp
            WHERE NOT EXISTS (
                SELECT `sha1`
                FROM neos_flow_resourcemanagement_persistentresource
                WHERE neos_flow_resourcemanagement_persistentresource.`sha1` = flownative_google_cloudstorage_temp.`sha1`
            );
        SQL;

        $result = $connection->executeQuery($query);
        $obsoleteObjectsCount = $result->rowCount();
        if ($obsoleteObjectsCount > 0) {
            $this->outputLine('<hint>Found </hint>%s<hint> objects in Google Cloud Storage which have no corresponding Persistent Resource object</hint>', [$result->rowCount()]);
            $row = $result->fetchAssociative();
            $this->outputLine('For example, the object with SHA1 <b>%s</b> is likely obsolete and can be deleted from the bucket', [$row['sha1']]);
            $this->outputLine();

            if ($exportToFile !== '') {
                $this->outputLine('<success>Exporting hashes of obsolete objects to "%s" ...</success>', [$exportToFile]);
                $this->exportObsoleteObjectHashesToFile($exportToFile, $connection, $query);
            } else {
                if (!$this->output->askConfirmation(sprintf('<error>Proceed with deletion of %s obsolete objects in Google Cloud Storage?</error> ', $result->rowCount()), false)) {
                    $this->outputLine('Nothing was deleted');
                    $this->dropTemporaryTable($connection);
                    exit;
                }
                $this->deleteObsoleteObjects($storage, $connection, $query, $obsoleteObjectsCount);
            }
        } else {
            $this->outputLine('<success>Found no obsolete objects in this Google Cloud Storage bucket</success>');
        }

        $this->dropTemporaryTable($connection);
        $this->outputLine('Done, memory peak usage was %s MB', [(string)(round(memory_get_peak_usage(true) / 1000000))]);
    }

    /**
     * @throws Exception
     */
    private function retrieveStorageObjectsResourcesHashes(GcsStorage $storage, Connection $connection): int
    {
        $this->outputLine();
        $this->outputLine('<success>Retrieving list of objects from Google Cloud Storage ...</success>', [$storage->getBucketName()]);

        try {
            $storageClient = $this->storageFactory->create();
        } catch (\Exception $e) {
            $this->outputLine('<error>%s</error>', [$e->getMessage()]);
            exit(1);
        }

        $storageBucket = $storageClient->bucket($storage->getBucketName());
        $storageKeyPrefix = $storage->getKeyPrefix();

        $this->output->progressStart();
        $storageObjectsCount = 0;

        $connection->executeQuery('TRUNCATE TABLE ' . self::TEMPORARY_TABLE_NAME);
        foreach ($storageBucket->objects(['prefix' => $storageKeyPrefix])->iterateByPage() as $pageNumber => $objects) {
            foreach ($objects as $object) {
                assert($object instanceof \Google\Cloud\Storage\StorageObject);
                $storageObjectsCount++;
                $connection->insert(self::TEMPORARY_TABLE_NAME, ['sha1' => $object->name()]);
            }
            $this->output->progressSet($storageObjectsCount);
        }

        $this->output->progressFinish();
        $this->outputLine();
        return $storageObjectsCount;
    }

    private function getStorageFromCollection(string $collectionName): GcsStorage
    {
        $collection = $this->resourceManager->getCollection($collectionName);
        if (!$collection) {
            $this->outputLine('<error>The collection %s does not exist.</error>', [$collectionName]);
            exit(1);
        }

        $storage = $collection->getStorage();
        if (!$storage instanceof GcsStorage) {
            $this->outputLine('<error>The storage defined in collection %s is not a Google Cloud Storage storage.</error>', [$collectionName]);
            exit(1);
        }
        return $storage;
    }

    private function getDatabaseConnection(): Connection
    {
        if ($this->objectManager->isRegistered(EntityManagerInterface::class)) {
            $entityManager = $this->objectManager->get(EntityManagerInterface::class);
        } else {
            $entityManager = $this->objectManager->get(\Doctrine\Common\Persistence\ObjectManager::class);
        }
        return $entityManager->getConnection();
    }

    private function createTemporaryTable(Connection $connection): void
    {
        $schemaManager = $connection->getSchemaManager();
        if ($schemaManager === null) {
            $this->outputLine('<error>Failed retrieving the schema manager from the DBAL connection</error>');
            exit(1);
        }
        if (!$schemaManager->tablesExist(self::TEMPORARY_TABLE_NAME)) {
            $schema = $schemaManager->createSchema();
            $table = $schema->createTable(self::TEMPORARY_TABLE_NAME);
            $table->addColumn('sha1', Types::STRING, ['length' => strlen(sha1('something'))]);
            $schemaManager->createTable($table);
        }
    }

    private function dropTemporaryTable(Connection $connection): void
    {
        $schemaManager = $connection->getSchemaManager();
        if ($schemaManager === null) {
            $this->outputLine('<error>Failed retrieving the schema manager from the DBAL connection</error>');
            exit(1);
        }
        $schemaManager->dropTable(self::TEMPORARY_TABLE_NAME);
    }

    private function exportObsoleteObjectHashesToFile(string $targetPathAndFilename, Connection $connection, string $query): void
    {
        try {
            $exportFile = fopen($targetPathAndFilename, 'wb');
            foreach ($connection->iterateAssociative($query) as $row) {
                fwrite($exportFile, $row['sha1'] . PHP_EOL);
            }
            fclose($exportFile);
        } catch (\Throwable $throwable) {
            $this->outputLine('<error>%s</error>', [$throwable->getMessage()]);
            exit(1);
        }
    }

    private function deleteObsoleteObjects(GcsStorage $storage, Connection $connection, string $query, int $obsoleteObjectsCount): void
    {
        try {
            $storageClient = $this->storageFactory->create();
            $storageBucket = $storageClient->bucket($storage->getBucketName());
            $storageKeyPrefix = $storage->getKeyPrefix();

            $this->output->progressStart($obsoleteObjectsCount);
            foreach ($connection->iterateAssociative($query) as $row) {
                $storageBucket->object($storageKeyPrefix . $row['sha1'])->delete();
                /** @noinspection DisconnectedForeachInstructionInspection */
                $this->output->progressAdvance();
            }
            $this->output->progressFinish();
            $this->outputLine();
        } catch (\Throwable $throwable) {
            $this->outputLine('<error>%s</error>', [$throwable->getMessage()]);
            exit(1);
        }

    }
}
