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

use Flownative\Google\CloudStorage\Exception;
use Flownative\Google\CloudStorage\GcsTarget;
use Flownative\Google\CloudStorage\StorageFactory;
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
    public function connectCommand(string $bucket)
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
    public function republishCommand(string $collection = 'persistent')
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
    public function updateResourceMetadataCommand(string $collection = 'persistent')
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
            foreach ($collection->getObjects() as $object) {
                /** @var StorageObject $object */
                $resource = $this->resourceManager->getResourceBySha1($object->getSha1());
                if ($resource) {
                    try {
                        $target->updateResourceMetadata($resource);
                        $this->outputLine('   ✅  %s %s ', [$resource->getSha1(), $resource->getFilename()]);
                    } catch (Exception $exception) {
                        $this->outputLine('   ❌  %s <error>%s</error>', [$resource->getSha1(), $exception->getMessage()]);
                    }
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
