<?php
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

use Doctrine\ORM\EntityManagerInterface;
use Flownative\Google\CloudStorage\GcsTarget;
use Flownative\Google\CloudStorage\StorageFactory;
use Google\Cloud\Core\Exception\NotFoundException;
use Google\Cloud\Core\Exception\ServiceException;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Cli\CommandController;
use Neos\Flow\ResourceManagement\ResourceManager;
use Neos\Flow\ResourceManagement\Storage\StorageObject;

/**
 * Google Cloud Storage command controller
 *
 * @Flow\Scope("singleton")
 */
final class GcsCommandController extends CommandController
{
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
     * @param string $collection Name of the collection to publish
     */
    public function updateResourceMetadataCommand(string $collection = 'persistent'): void
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
        $queryResult = $entityManager->getConnection()->executeQuery(
            'SELECT sha1, filename, mediatype FROM neos_flow_resourcemanagement_persistentresource AS r WHERE collectionname = :collectionName ORDER BY sha1',
            [
                'collectionName' => $collectionName
            ]
        );

        try {
            $previousSha1 = null;
            while ($resourceRecord = $queryResult->fetch(\PDO::FETCH_ASSOC)) {
                if ($resourceRecord['sha1'] === $previousSha1) {
                    continue;
                }
                $previousSha1 = $resourceRecord['sha1'];

                try {
                    $object = $targetBucket->object($targetKeyPrefix . $resourceRecord['sha1']);
                    $object->update(['contentType' => $resourceRecord['mediatype']]);
                    $this->outputLine('   ✅  %s %s ', [$resourceRecord['sha1'], $resourceRecord['filename']]);
                } catch (ServiceException | NotFoundException $exception) {
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
}
