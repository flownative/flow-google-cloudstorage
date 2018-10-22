<?php
namespace Flownative\Google\CloudStorage;

/*
 * This file is part of the Flownative.Google.CloudStorage package.
 *
 * (c) Robert Lemke, Flownative GmbH - www.flownative.com
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Flownative\Google\CloudStorage\Exception as CloudStorageException;
use Google\Cloud\Core\Exception\GoogleException;
use Google\Cloud\Core\Exception\NotFoundException;
use Google\Cloud\Core\Exception\ServiceException;
use Google\Cloud\Storage\Bucket;
use Google\Cloud\Storage\StorageClient;
use Google\Cloud\Storage\StorageObject;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Log\SystemLoggerInterface;
use Neos\Flow\ResourceManagement\CollectionInterface;
use Neos\Flow\ResourceManagement\Exception;
use Neos\Flow\ResourceManagement\PersistentResource;
use Neos\Flow\ResourceManagement\Publishing\MessageCollector;
use Neos\Flow\ResourceManagement\ResourceManager;
use Neos\Flow\ResourceManagement\ResourceMetaDataInterface;
use Neos\Flow\ResourceManagement\Target\TargetInterface;
use Neos\Flow\Utility\Environment;

/**
 * A resource publishing target based on Amazon S3
 */
class GcsTarget implements TargetInterface
{

    /**
     * Name which identifies this resource target
     *
     * @var string
     */
    protected $name;

    /**
     * Name of the S3 bucket which should be used for publication
     *
     * @var string
     */
    protected $bucketName;

    /**
     * A prefix to use for the key of bucket objects used by this storage
     *
     * @var string
     */
    protected $keyPrefix = '';

    /**
     * CORS (Cross-Origin Resource Sharing) allowed origins for published content
     *
     * @var string
     */
    protected $corsAllowOrigin = '*';

    /**
     * @var string
     */
    protected $baseUri;

    /**
     * @var int
     */
    protected $gzipCompressionLevel = 9;

    /**
     * @var string[]
     */
    protected $gzipCompressionMediaTypes = [
      'text/plain',
      'text/css',
      'text/xml',
      'text/mathml',
      'text/javascript',
      'application/x-javascript',
      'application/xml',
      'application/rss+xml',
      'application/atom+xml',
      'application/javascript',
      'application/json',
      'application/x-font-woff',
      'image/svg+xml'
    ];

    /**
     * Internal cache for known storages, indexed by storage name
     *
     * @var array<\Neos\Flow\ResourceManagement\Storage\StorageInterface>
     */
    protected $storages = array();

    /**
     * @Flow\Inject
     * @var SystemLoggerInterface
     */
    protected $systemLogger;

    /**
     * @Flow\Inject
     * @var ResourceManager
     */
    protected $resourceManager;

    /**
     * @Flow\Inject
     * @var MessageCollector
     */
    protected $messageCollector;

    /**
     * @Flow\Inject
     * @var StorageFactory
     */
    protected $storageFactory;

    /**
     * @var StorageClient
     */
    protected $storageClient;

    /**
     * @Flow\Inject
     * @var Environment
     */
    protected $environment;

    /**
     * @var Bucket
     */
    protected $currentBucket;

    /**
     * @var array
     */
    protected $existingObjectsInfo;

    /**
     * @var bool
     */
    protected $bucketIsPublic;

    /**
     * Constructor
     *
     * @param string $name Name of this target instance, according to the resource settings
     * @param array $options Options for this target
     * @throws Exception
     */
    public function __construct($name, array $options = array())
    {
        $this->name = $name;
        foreach ($options as $key => $value) {
            switch ($key) {
                case 'bucket':
                    $this->bucketName = $value;
                break;
                case 'keyPrefix':
                    $this->keyPrefix = ltrim($value, '/');
                break;
                case 'corsAllowOrigin':
                    $this->corsAllowOrigin = $value;
                break;
                case 'baseUri':
                    $this->baseUri = $value;
                break;
                case 'gzipCompressionLevel':
                    $this->gzipCompressionLevel = intval($value);
                break;
                case 'gzipCompressionMediaTypes':
                    if (!is_array($value)) {
                        throw new Exception(sprintf('The option "%s" which was specified in the configuration of the "%s" resource GcsTarget is not a valid array. Please check your settings.', $key, $name), 1520267221740);
                    }
                    foreach ($value as $mediaType) {
                        if (!is_string($mediaType)) {
                            throw new Exception(sprintf('The option "%s" which was specified in the configuration of the "%s" resource GcsTarget is not a valid array of strings. Please check your settings.', $key, $name), 1520267338243);
                        }
                    }
                    $this->gzipCompressionMediaTypes = $value;
                break;
                default:
                    if ($value !== null) {
                        throw new Exception(sprintf('An unknown option "%s" was specified in the configuration of the "%s" resource GcsTarget. Please check your settings.', $key, $name), 1446719852);
                    }
            }
        }
    }

    /**
     * Initialize the Google Cloud Storage instance
     *
     * @return void
     * @throws CloudStorageException
     */
    public function initializeObject()
    {
        $this->storageClient = $this->storageFactory->create();
    }

    /**
     * Returns the name of this target instance
     *
     * @return string The target instance name
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * Returns the S3 object key prefix
     *
     * @return string
     */
    public function getKeyPrefix()
    {
        return $this->keyPrefix;
    }

    /**
     * @return string
     */
    public function getBucketName(): string
    {
        return $this->bucketName;
    }

    /**
     * Publishes the whole collection to this target
     *
     * @param CollectionInterface $collection The collection to publish
     * @throws \Exception
     * @throws \Neos\Flow\Exception
     */
    public function publishCollection(CollectionInterface $collection)
    {
        $storage = $collection->getStorage();
        $targetBucket = $this->getCurrentBucket();

        if ($storage instanceof GcsStorage && $storage->getBucketName() === $targetBucket->name()) {
            // Nothing to do: the storage bucket is (or should be) publicly accessible
            return;
        }

        if (!isset($this->existingObjectsInfo)) {
            $this->existingObjectsInfo = [];
            $parameters = [
                'prefix' => $this->keyPrefix
            ];

            foreach ($this->getCurrentBucket()->objects($parameters) as $storageObject) {
                /** @var StorageObject $storageObject */
                $this->existingObjectsInfo[$storageObject->name()] = true;
            }
        }

        $obsoleteObjects = $this->existingObjectsInfo;

        if ($storage instanceof GcsStorage) {
            $this->publishCollectionFromDifferentGoogleCloudStorage($collection, $storage, $this->existingObjectsInfo, $obsoleteObjects, $targetBucket);
        } else {
            foreach ($collection->getObjects() as $object) {
                /** @var \Neos\Flow\ResourceManagement\Storage\StorageObject $object */
                $this->publishFile($object->getStream(), $this->getRelativePublicationPathAndFilename($object), $object);
                unset($obsoleteObjects[$this->getRelativePublicationPathAndFilename($object)]);
            }
        }

        $this->systemLogger->log(sprintf('Removing %s obsolete objects from target bucket "%s".', count($obsoleteObjects), $this->bucketName), LOG_INFO);
        foreach (array_keys($obsoleteObjects) as $relativePathAndFilename) {
            try {
                $targetBucket->object($this->keyPrefix . $relativePathAndFilename)->delete();
            } catch (NotFoundException $e) {
            }
        }
    }

    /**
     * @param CollectionInterface $collection
     * @param GcsStorage $storage
     * @param array $existingObjects
     * @param array $obsoleteObjects
     * @param Bucket $targetBucket
     * @throws \Neos\Flow\Exception
     */
    private function publishCollectionFromDifferentGoogleCloudStorage(CollectionInterface $collection, GcsStorage $storage, array $existingObjects, array &$obsoleteObjects, Bucket $targetBucket)
    {
        $storageBucketName = $storage->getBucketName();
        $storageBucket = $this->storageClient->bucket($storageBucketName);
        $iteration = 0;

        $this->systemLogger->log(sprintf('Found %s existing objects in target bucket "%s".', count($existingObjects), $this->bucketName), LOG_INFO);

        foreach ($collection->getObjects() as $object) {
            /** @var \Neos\Flow\ResourceManagement\Storage\StorageObject $object */
            $targetObjectName = $this->keyPrefix . $this->getRelativePublicationPathAndFilename($object);
            if (isset($existingObjects[$targetObjectName])) {
                $this->systemLogger->log(sprintf('Skipping object "%s" because it already exists in bucket "%s"', $targetObjectName, $this->bucketName), LOG_DEBUG);
                unset($obsoleteObjects[$targetObjectName]);
                continue;
            }

            if (in_array($object->getMediaType(), $this->gzipCompressionMediaTypes)) {
                try {
                    $this->publishFile($object->getStream(), $this->getRelativePublicationPathAndFilename($object), $object);
                } catch (\Exception $e) {
                    $this->messageCollector->append(sprintf('Could not publish resource with SHA1 hash %s of collection %s from bucket %s to %s: %s', $object->getSha1(), $collection->getName(), $storageBucketName, $this->bucketName, $e->getMessage()));
                }
                $this->systemLogger->log(sprintf('Successfully copied resource as object "%s" (MD5: %s) from bucket "%s" to bucket "%s" (with GZIP compression)', $targetObjectName, $object->getMd5() ?: 'unknown', $storageBucketName, $this->bucketName), LOG_DEBUG);
            } else {
                try {
                    $this->systemLogger->log(sprintf('Copy object "%s" to bucket "%s"', $targetObjectName, $this->bucketName), LOG_DEBUG);
                    $options = [
                        'name' => $targetObjectName,
                        'predefinedAcl' => 'publicRead',
                        'contentType' => $object->getMediaType(),
                        'cacheControl' => 'public, max-age=1209600',
                    ];

                    $storageBucket->object($storage->getKeyPrefix() . $object->getSha1())->copy($targetBucket, $options);
                } catch (GoogleException $e) {
                    $googleError = json_decode($e->getMessage());
                    if ($googleError instanceof \stdClass && isset($googleError->error->message)) {
                        $this->messageCollector->append(sprintf('Could not copy resource with SHA1 hash %s of collection %s from bucket %s to %s: %s', $object->getSha1(), $collection->getName(), $storageBucketName, $this->bucketName, $googleError->error->message));
                    } else {
                        $this->messageCollector->append(sprintf('Could not copy resource with SHA1 hash %s of collection %s from bucket %s to %s: %s', $object->getSha1(), $collection->getName(), $storageBucketName, $this->bucketName, $e->getMessage()));
                    }
                    continue;
                }
                $this->systemLogger->log(sprintf('Successfully copied resource as object "%s" (MD5: %s) from bucket "%s" to bucket "%s"', $targetObjectName, $object->getMd5() ?: 'unknown', $storageBucketName, $this->bucketName), LOG_DEBUG);
            }
            unset($targetObjectName);
            $iteration ++;
        }

        $this->systemLogger->log(sprintf('Published %s new objects to target bucket "%s".', $iteration, $this->bucketName), LOG_INFO);
    }

    /**
     * Returns the web accessible URI pointing to the given static resource
     *
     * @param string $relativePathAndFilename Relative path and filename of the static resource
     * @return string The URI
     */
    public function getPublicStaticResourceUri($relativePathAndFilename)
    {
        $relativePathAndFilename = $this->encodeRelativePathAndFilenameForUri($relativePathAndFilename);
        return 'https://storage.googleapis.com/' . $this->bucketName . '/'. $this->keyPrefix . $relativePathAndFilename;
    }

    /**
     * Publishes the given persistent resource from the given storage
     *
     * @param \Neos\Flow\ResourceManagement\PersistentResource $resource The resource to publish
     * @param CollectionInterface $collection The collection the given resource belongs to
     * @return void
     * @throws Exception
     * @throws \Exception
     */
    public function publishResource(PersistentResource $resource, CollectionInterface $collection)
    {
        $storage = $collection->getStorage();
        if ($storage instanceof GcsStorage && $storage->getBucketName() === $this->bucketName) {
            $updated = false;
            $retries = 0;
            while (!$updated) {
                try {
                    $storageBucket = $this->storageClient->bucket($storage->getBucketName());
                    $storageBucket->object($storage->getKeyPrefix() . $resource->getSha1())->update(['contentType' => $resource->getMediaType()]);
                    $updated = true;
                } catch (GoogleException $exception) {
                    $retries ++;
                    if ($retries > 10) {
                        throw $exception;
                    }
                    usleep(10 * 2 ^ $retries);
                }
            }
            return;
        }

        if ($storage instanceof GcsStorage && !in_array($resource->getMediaType(), $this->gzipCompressionMediaTypes)) {
            if ($storage->getBucketName() === $this->bucketName && $storage->getKeyPrefix() === $this->keyPrefix) {
                throw new Exception(sprintf('Could not publish resource with SHA1 hash %s of collection %s because the source and target bucket is the same, with identical key prefixes. Either choose a different bucket or at least key prefix for the target.', $resource->getSha1(), $collection->getName()), 1446721574);
            }
            $targetObjectName = $this->keyPrefix . $this->getRelativePublicationPathAndFilename($resource);
            $storageBucket = $this->storageClient->bucket($storage->getBucketName());

            try {
                $storageBucket->object($storage->getKeyPrefix() . $resource->getSha1())->copy($this->getCurrentBucket(), [
                    'name' => $targetObjectName,
                    'predefinedAcl' => 'publicRead',
                    'contentType' => $resource->getMediaType(),
                    'cacheControl' => 'public, max-age=1209600',
                ]);

            } catch (GoogleException $e) {
                $googleError = json_decode($e->getMessage());
                if ($googleError instanceof \stdClass && isset($googleError->error->message)) {
                    $this->messageCollector->append(sprintf('Could not copy resource with SHA1 hash %s of collection %s from bucket %s to %s: %s', $resource->getSha1(), $collection->getName(), $storage->getBucketName(), $this->bucketName, $googleError->error->message), LOG_ERR, 1446721791);
                } else {
                    $this->messageCollector->append(sprintf('Could not copy resource with SHA1 hash %s of collection %s from bucket %s to %s: %s', $resource->getSha1(), $collection->getName(), $storage->getBucketName(), $this->bucketName, $e->getMessage()), LOG_ERR, 1446721791);
                }
                return;
            }

            $this->systemLogger->log(sprintf('Successfully published resource as object "%s" (MD5: %s) by copying from bucket "%s" to bucket "%s"', $targetObjectName, $resource->getMd5() ?: 'unknown', $storage->getBucketName(), $this->bucketName), LOG_DEBUG);
        } else {
            $sourceStream = $resource->getStream();
            if ($sourceStream === false) {
                $this->messageCollector->append(sprintf('Could not publish resource with SHA1 hash %s of collection %s because there seems to be no corresponding data in the storage.', $resource->getSha1(), $collection->getName()), LOG_ERR, 1446721810);
                return;
            }
            $this->publishFile($sourceStream, $this->getRelativePublicationPathAndFilename($resource), $resource);
        }
    }

    /**
     * Updates the metadata (currently content type) of a resource object already stored in this target
     *
     * @param PersistentResource $resource
     * @throws \Flownative\Google\CloudStorage\Exception
     */
    public function updateResourceMetadata(PersistentResource $resource)
    {
        try {
            $targetBucket = $this->storageClient->bucket($this->bucketName);
        } catch (NotFoundException $exception) {
            throw new \Flownative\Google\CloudStorage\Exception(sprintf('Failed retrieving bucket information for "%s".', $this->bucketName), 1538462744);
        }
        try {
            $object = $targetBucket->object($this->getKeyPrefix() . $resource->getSha1());
            $object->update(['contentType' => $resource->getMediaType()]);
        } catch (ServiceException | NotFoundException $exception) {
            throw new \Flownative\Google\CloudStorage\Exception(sprintf('Resource "%s" (%s) not found in bucket %s.', $resource->getSha1(), $resource->getFilename(), $this->bucketName), 1538462744);
        }
    }

    /**
     * Unpublishes the given persistent resource
     *
     * @param \Neos\Flow\ResourceManagement\PersistentResource $resource The resource to unpublish
     * @throws \Exception
     */
    public function unpublishResource(PersistentResource $resource)
    {
        $collection = $this->resourceManager->getCollection($resource->getCollectionName());
        $storage = $collection->getStorage();
        if ($storage instanceof GcsStorage && $storage->getBucketName() === $this->bucketName) {
            // Unpublish for same-bucket setups is a NOOP, because the storage object will already be deleted.
            return;
        }

        try {
            $objectName = $this->keyPrefix . $this->getRelativePublicationPathAndFilename($resource);
            $this->getCurrentBucket()->object($objectName)->delete();
            $this->systemLogger->log(sprintf('Successfully unpublished resource as object "%s" (MD5: %s) from bucket "%s"', $objectName, $resource->getMd5() ?: 'unknown', $this->bucketName), LOG_DEBUG);
        } catch (NotFoundException $e) {
        }
    }

    /**
     * Returns the web accessible URI pointing to the specified persistent resource
     *
     * @param \Neos\Flow\ResourceManagement\PersistentResource $resource PersistentResource object or the resource hash of the resource
     * @return string The URI
     */
    public function getPublicPersistentResourceUri(PersistentResource $resource)
    {
        $relativePathAndFilename = $this->encodeRelativePathAndFilenameForUri($this->getRelativePublicationPathAndFilename($resource));
        if ($this->baseUri != '') {
            return $this->baseUri . $relativePathAndFilename;
        } else {
            return 'https://storage.googleapis.com/' . $this->bucketName . '/'. $this->keyPrefix . $relativePathAndFilename;
        }
    }

    /**
     * Publishes the specified source file to this target, with the given relative path.
     *
     * @param resource $sourceStream
     * @param string $relativeTargetPathAndFilename
     * @param ResourceMetaDataInterface $metaData
     * @throws \Exception
     */
    protected function publishFile($sourceStream, $relativeTargetPathAndFilename, ResourceMetaDataInterface $metaData)
    {
        $objectName = $this->keyPrefix . $relativeTargetPathAndFilename;
        $uploadParameters =  [
            'name' => $objectName,
            'predefinedAcl' => 'publicRead',
            'contentType' => $metaData->getMediaType(),
            'cacheControl' => 'public, max-age=1209600'
        ];

        if (in_array($metaData->getMediaType(), $this->gzipCompressionMediaTypes)) {
            try {
                $temporaryTargetPathAndFilename = $this->environment->getPathToTemporaryDirectory() . uniqid('Flownative_Google_CloudStorage_');
                $temporaryTargetStream = gzopen($temporaryTargetPathAndFilename, 'wb' . $this->gzipCompressionLevel);
                while(!feof($sourceStream)) {
                    gzwrite($temporaryTargetStream, fread($sourceStream, 524288));
                }
                fclose($sourceStream);
                fclose($temporaryTargetStream);

                $sourceStream = fopen($temporaryTargetPathAndFilename, 'rb');
                $uploadParameters['metadata']['contentEncoding'] = 'gzip';

                $this->systemLogger->log(sprintf('Converted resource data of object "%s" in bucket "%s" with MD5 hash "%s" to GZIP with level %s.', $objectName, $this->bucketName, $metaData->getMd5() ?: 'unknown', $this->gzipCompressionLevel), LOG_DEBUG);
            } catch (\Exception $e) {
                $this->messageCollector->append(sprintf('Failed publishing resource as object "%s" in bucket "%s" with MD5 hash "%s": %s', $objectName, $this->bucketName, $metaData->getMd5() ?: 'unknown', $e->getMessage()), LOG_WARNING, 1520257344878);
            }
        }
        try {
            $this->getCurrentBucket()->upload($sourceStream, $uploadParameters);
            $this->systemLogger->log(sprintf('Successfully published resource as object "%s" in bucket "%s" with MD5 hash "%s"', $objectName, $this->bucketName, $metaData->getMd5() ?: 'unknown'), LOG_DEBUG);
        } catch (\Exception $e) {
            $this->messageCollector->append(sprintf('Failed publishing resource as object "%s" in bucket "%s" with MD5 hash "%s": %s', $objectName, $this->bucketName, $metaData->getMd5() ?: 'unknown', $e->getMessage()), LOG_WARNING, 1506847965352);
        } finally {
            if (is_resource($sourceStream)) {
                fclose($sourceStream);
            }
            if (isset($temporaryTargetPathAndFilename) && file_exists($temporaryTargetPathAndFilename)) {
                unlink ($temporaryTargetPathAndFilename);
            }
        }
    }

    /**
     * Determines and returns the relative path and filename for the given Storage Object or PersistentResource. If the given
     * object represents a persistent resource, its own relative publication path will be empty. If the given object
     * represents a static resources, it will contain a relative path.
     *
     * @param ResourceMetaDataInterface $object PersistentResource or Storage Object
     * @return string The relative path and filename, for example "c828d0f88ce197be1aff7cc2e5e86b1244241ac6/MyPicture.jpg"
     */
    protected function getRelativePublicationPathAndFilename(ResourceMetaDataInterface $object)
    {
        if ($object->getRelativePublicationPath() !== '') {
            $pathAndFilename = $object->getRelativePublicationPath() . $object->getFilename();
        } else {
            $pathAndFilename = $object->getSha1() . '/' . $object->getFilename();
        }
        return $pathAndFilename;
    }

    /**
     * Applies rawurlencode() to all path segments of the given $relativePathAndFilename
     *
     * @param string $relativePathAndFilename
     * @return string
     */
    protected function encodeRelativePathAndFilenameForUri($relativePathAndFilename)
    {
        return implode('/', array_map('rawurlencode', explode('/', $relativePathAndFilename)));
    }

    /**
     * @return Bucket
     */
    protected function getCurrentBucket()
    {
        if ($this->currentBucket === null) {
            $this->currentBucket = $this->storageClient->bucket($this->bucketName);
        }
        return $this->currentBucket;
    }
}

