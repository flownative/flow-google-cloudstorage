<?php
declare(strict_types=1);

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
use Google\Cloud\Storage\Bucket;
use Google\Cloud\Storage\StorageClient;
use Google\Cloud\Storage\StorageObject;
use GuzzleHttp\Psr7\Uri;
use Neos\Error\Messages\Error;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Log\Utility\LogEnvironment;
use Neos\Flow\ObjectManagement\ObjectManagerInterface;
use Neos\Flow\ResourceManagement\CollectionInterface;
use Neos\Flow\ResourceManagement\Exception;
use Neos\Flow\ResourceManagement\PersistentResource;
use Neos\Flow\ResourceManagement\Publishing\MessageCollector;
use Neos\Flow\ResourceManagement\ResourceManager;
use Neos\Flow\ResourceManagement\ResourceMetaDataInterface;
use Neos\Flow\ResourceManagement\Target\TargetInterface;
use Neos\Flow\Utility\Environment;
use Psr\Log\LoggerInterface;

/**
 * A resource publishing target based on Google Cloud Storage
 */
class GcsTarget implements TargetInterface
{

    /**
     * Name which identifies this resource target
     *
     * @var string
     */
    protected $name = '';

    /**
     * Name of the Google Cloud Storage bucket which should be used for publication
     *
     * @var string
     */
    protected $bucketName = '';

    /**
     * A prefix to use for the key of bucket objects used by this storage
     *
     * @var string
     */
    protected $keyPrefix = '';

    /**
     * @var string
     */
    protected $persistentResourceUriPattern = '';

    /**
     * @string
     */
    private const DEFAULT_PERSISTENT_RESOURCE_URI_PATTERN = '{baseUri}{keyPrefix}{sha1}/{filename}';

    /**
     * @var bool
     */
    protected $persistentResourceUriEnableSigning = false;

    /**
     * @var int
     */
    protected $persistentResourceUriSignatureLifetime = 600;

    /**
     * CORS (Cross-Origin Resource Sharing) allowed origins for published content
     *
     * @var string
     */
    protected $corsAllowOrigin = '*';

    /**
     * @var string
     */
    protected $baseUri = '';

    /**
     * @var array
     */
    protected $customBaseUriMethod = [];

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
     * @Flow\Inject
     * @var LoggerInterface
     */
    protected $logger;

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
     * @Flow\Inject
     * @var ObjectManagerInterface
     */
    protected $objectManager;

    /**
     * Constructor
     *
     * @param string $name Name of this target instance, according to the resource settings
     * @param array $options Options for this target
     * @throws Exception
     */
    public function __construct($name, array $options = [])
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
                case 'persistentResourceUris':
                    if (!is_array($value)) {
                        throw new Exception(sprintf('The option "%s" which was specified in the configuration of the "%s" resource GcsTarget is not a valid array. Please check your settings.', $key, $name), 1568875196);
                    }
                    foreach ($value as $uriOptionKey => $uriOptionValue) {
                        switch ($uriOptionKey) {
                            case 'pattern':
                                $this->persistentResourceUriPattern = (string)$uriOptionValue;
                            break;
                            case 'enableSigning':
                                $this->persistentResourceUriEnableSigning = (bool)$uriOptionValue;
                            break;
                            case 'signatureLifetime':
                                $this->persistentResourceUriSignatureLifetime = (int)$uriOptionValue;
                            break;
                            default:
                                if ($value !== null) {
                                    throw new Exception(sprintf('An unknown option "%s" was specified in the configuration of the "%s" resource GcsTarget. Please check your settings.', $uriOptionKey, $name), 1568876031);
                                }
                        }
                    }
                break;
                case 'corsAllowOrigin':
                    $this->corsAllowOrigin = $value;
                break;
                case 'baseUri':
                    if (!empty($value)) {
                        $this->baseUri = $value;
                    }
                break;
                case 'customBaseUriMethod':
                    if (!is_array($value)) {
                        throw new Exception(sprintf('The option "%s" which was specified in the configuration of the "%s" resource GcsTarget is not a valid array. Please check your settings.', $key, $name), 1569227014);
                    }
                    if (!isset($value['objectName'], $value['methodName'])) {
                        throw new Exception(sprintf('The option "%s" which was specified in the configuration of the "%s" resource GcsTarget requires exactly two keys ("objectName" and "methodName"). Please check your settings.', $key, $name), 1569227112);
                    }
                    $this->customBaseUriMethod = $value;
                break;
                case 'gzipCompressionLevel':
                    $this->gzipCompressionLevel = (int)$value;
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
     * @throws Exception
     */
    public function initializeObject(): void
    {
        $this->storageClient = $this->storageFactory->create();
        if ($this->customBaseUriMethod !== []) {
            if (!$this->objectManager->isRegistered($this->customBaseUriMethod['objectName'])) {
                throw new Exception(sprintf('Unknown object "%s" defined as custom base URI method in the configuration of the "%s" resource GcsTarget. Please check your settings.', $this->customBaseUriMethod['objectName'], $this->name), 1569228841);
            }
            $object = $this->objectManager->get($this->customBaseUriMethod['objectName']);
            $methodName = $this->customBaseUriMethod['methodName'];
            if (!method_exists($object, $methodName)) {
                throw new Exception(sprintf('Unknown method "%s->%s" defined as custom base URI method in the configuration of the "%s" resource GcsTarget. Please check your settings.', $this->customBaseUriMethod['objectName'], $methodName, $this->name), 1569228902);
            }
            $this->baseUri = $object->$methodName(
                [
                    'targetClass' => get_class($this),
                    'bucketName' => $this->bucketName,
                    'keyPrefix' => $this->keyPrefix,
                    'baseUri' => $this->baseUri,
                    'persistentResourceUriEnableSigning' => $this->persistentResourceUriEnableSigning
                ]
            );
        }
    }

    /**
     * Returns the name of this target instance
     *
     * @return string The target instance name
     */
    public function getName(): string
    {
        return $this->name;
    }

    /**
     * Returns the object key prefix
     *
     * @return string
     */
    public function getKeyPrefix(): string
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
     * @return void
     * @throws \Exception
     * @throws \Neos\Flow\Exception
     */
    public function publishCollection(CollectionInterface $collection): void
    {
        $storage = $collection->getStorage();
        $targetBucket = $this->getCurrentBucket();

        if ($this->isOneBucketSetup($collection)) {
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

        $this->logger->info(sprintf('Removing %s obsolete objects from target bucket "%s".', count($obsoleteObjects), $this->bucketName), LogEnvironment::fromMethodName(__METHOD__));
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
     * @return void
     * @throws \Neos\Flow\Exception
     */
    private function publishCollectionFromDifferentGoogleCloudStorage(CollectionInterface $collection, GcsStorage $storage, array $existingObjects, array &$obsoleteObjects, Bucket $targetBucket): void
    {
        $storageBucketName = $storage->getBucketName();
        $storageBucket = $this->storageClient->bucket($storageBucketName);
        $iteration = 0;

        $this->logger->info(sprintf('Found %s existing objects in target bucket "%s".', count($existingObjects), $this->bucketName), LogEnvironment::fromMethodName(__METHOD__));

        foreach ($collection->getObjects() as $object) {
            /** @var \Neos\Flow\ResourceManagement\Storage\StorageObject $object */
            $targetObjectName = $this->keyPrefix . $this->getRelativePublicationPathAndFilename($object);
            if (isset($existingObjects[$targetObjectName])) {
                $this->logger->debug(sprintf('Skipping object "%s" because it already exists in bucket "%s"', $targetObjectName, $this->bucketName), LogEnvironment::fromMethodName(__METHOD__));
                unset($obsoleteObjects[$targetObjectName]);
                continue;
            }

            if (in_array($object->getMediaType(), $this->gzipCompressionMediaTypes, true)) {
                try {
                    $this->publishFile($object->getStream(), $this->getRelativePublicationPathAndFilename($object), $object);
                } catch (\Exception $e) {
                    $this->messageCollector->append(sprintf('Could not publish resource with SHA1 hash %s of collection %s from bucket %s to %s: %s', $object->getSha1(), $collection->getName(), $storageBucketName, $this->bucketName, $e->getMessage()));
                }
                $this->logger->debug(sprintf('Successfully copied resource as object "%s" (SHA1: %s) from bucket "%s" to bucket "%s" (with GZIP compression)', $targetObjectName, $object->getSha1() ?: 'unknown', $storageBucketName, $this->bucketName), LogEnvironment::fromMethodName(__METHOD__));
            } else {
                try {
                    $this->logger->debug(sprintf('Copy object "%s" to bucket "%s"', $targetObjectName, $this->bucketName), LogEnvironment::fromMethodName(__METHOD__));
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
                $this->logger->debug(sprintf('Successfully copied resource as object "%s" (SHA1: %s) from bucket "%s" to bucket "%s"', $targetObjectName, $object->getSha1() ?: 'unknown', $storageBucketName, $this->bucketName), LogEnvironment::fromMethodName(__METHOD__));
            }
            unset($targetObjectName);
            $iteration++;
        }

        $this->logger->info(sprintf('Published %s new objects to target bucket "%s".', $iteration, $this->bucketName), LogEnvironment::fromMethodName(__METHOD__));
    }

    /**
     * Returns the web accessible URI pointing to the given static resource
     *
     * @param string $relativePathAndFilename Relative path and filename of the static resource
     * @return string The URI
     */
    public function getPublicStaticResourceUri($relativePathAndFilename): string
    {
        $relativePathAndFilename = $this->encodeRelativePathAndFilenameForUri($relativePathAndFilename);
        return 'https://storage.googleapis.com/' . $this->bucketName . '/' . $this->keyPrefix . $relativePathAndFilename;
    }

    /**
     * Publishes the given persistent resource from the given storage
     *
     * @param PersistentResource $resource The resource to publish
     * @param CollectionInterface $collection The collection the given resource belongs to
     * @return void
     * @throws Exception
     * @throws \Exception
     */
    public function publishResource(PersistentResource $resource, CollectionInterface $collection): void
    {
        $storage = $collection->getStorage();
        if ($storage instanceof GcsStorage && $storage->getBucketName() === $this->bucketName) {
            $updated = false;
            $retries = 0;
            while (!$updated) {
                try {
                    $storageBucket = $this->storageClient->bucket($storage->getBucketName());
                    $storageBucket->object($storage->getKeyPrefix() . $resource->getSha1())->update(
                        [
                            'predefinedAcl' => 'publicRead',
                            'contentType' => $resource->getMediaType(),
                            'cacheControl' => 'public, max-age=1209600'
                        ]);
                    $updated = true;
                } catch (GoogleException $exception) {
                    $retries++;
                    if ($retries > 10) {
                        throw $exception;
                    }
                    usleep(10 * 2 ^ $retries);
                }
            }
            return;
        }

        if ($storage instanceof GcsStorage && !in_array($resource->getMediaType(), $this->gzipCompressionMediaTypes, true)) {
            if ($this->isOneBucketSetup($collection)) {
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
                    $this->messageCollector->append(sprintf('Could not copy resource with SHA1 hash %s of collection %s from bucket %s to %s: %s', $resource->getSha1(), $collection->getName(), $storage->getBucketName(), $this->bucketName, $googleError->error->message), Error::SEVERITY_ERROR, 1446721791);
                } else {
                    $this->messageCollector->append(sprintf('Could not copy resource with SHA1 hash %s of collection %s from bucket %s to %s: %s', $resource->getSha1(), $collection->getName(), $storage->getBucketName(), $this->bucketName, $e->getMessage()), Error::SEVERITY_ERROR, 1446721791);
                }
                return;
            }

            $this->logger->debug(sprintf('Successfully published resource as object "%s" (SHA1: %s) by copying from bucket "%s" to bucket "%s"', $targetObjectName, $resource->getSha1() ?: 'unknown', $storage->getBucketName(), $this->bucketName), LogEnvironment::fromMethodName(__METHOD__));
        } else {
            $sourceStream = $resource->getStream();
            if ($sourceStream === false) {
                $this->messageCollector->append(sprintf('Could not publish resource with SHA1 hash %s of collection %s because there seems to be no corresponding data in the storage.', $resource->getSha1(), $collection->getName()), Error::SEVERITY_ERROR, 1446721810);
                return;
            }
            $this->publishFile($sourceStream, $this->getRelativePublicationPathAndFilename($resource), $resource);
        }
    }

    /**
     * Unpublishes the given persistent resource
     *
     * @param PersistentResource $resource The resource to unpublish
     * @throws \Exception
     */
    public function unpublishResource(PersistentResource $resource): void
    {
        $collection = $this->resourceManager->getCollection($resource->getCollectionName());
        if ($this->isOneBucketSetup($collection)) {
            // Unpublish for same-bucket setups is a NOOP, because the storage object will already be deleted.
            return;
        }

        try {
            $objectName = $this->keyPrefix . $this->getRelativePublicationPathAndFilename($resource);
            $this->getCurrentBucket()->object($objectName)->delete();
            $this->logger->debug(sprintf('Successfully unpublished resource as object "%s" (SHA1: %s) from bucket "%s"', $objectName, $resource->getSha1() ?: 'unknown', $this->bucketName), LogEnvironment::fromMethodName(__METHOD__));
        } catch (NotFoundException $e) {
        }
    }

    /**
     * Returns the web accessible URI pointing to the specified persistent resource
     *
     * @param PersistentResource $resource PersistentResource object or the resource hash of the resource
     * @return string The URI
     */
    public function getPublicPersistentResourceUri(PersistentResource $resource): string
    {
        $baseUri = $this->baseUri;
        $customUri = $this->persistentResourceUriPattern;
        if (empty($customUri)) {
            if (empty($baseUri)) {
                $baseUri = 'https://storage.googleapis.com/';
                $customUri = '{baseUri}{bucketName}/{keyPrefix}{sha1}/{filename}';
            } else {
                $customUri = self::DEFAULT_PERSISTENT_RESOURCE_URI_PATTERN;
            }
        }

        $variables = [
            '{baseUri}' => $baseUri,
            '{bucketName}' => $this->bucketName,
            '{keyPrefix}' => $this->keyPrefix,
            '{sha1}' => $resource->getSha1(),
            '{filename}' => $resource->getFilename(),
            '{fileExtension}' => $resource->getFileExtension()
        ];

        if (method_exists($resource, 'getMd5')) {
            $variables['{md5}'] = $resource->getMd5();
        }

        foreach ($variables as $placeholder => $replacement) {
            $customUri = str_replace($placeholder, $replacement, $customUri);
        }

        if ($this->persistentResourceUriEnableSigning) {
            $objectName = $this->keyPrefix . $resource->getSha1();
            $signedStandardUri = new Uri($this->getCurrentBucket()->object($objectName)->signedUrl(time() + $this->persistentResourceUriSignatureLifetime, ['method' => 'GET']));
            $customUri .= '?' . $signedStandardUri->getQuery();
        }

        // Let Uri implementation take care of encoding the Uri
        $uri = new Uri($customUri);
        return (string)$uri;
    }

    /**
     * Publishes the specified source file to this target, with the given relative path.
     *
     * @param resource $sourceStream
     * @param string $relativeTargetPathAndFilename
     * @param ResourceMetaDataInterface $metaData
     * @throws \Exception
     */
    protected function publishFile($sourceStream, string $relativeTargetPathAndFilename, ResourceMetaDataInterface $metaData): void
    {
        $objectName = $this->keyPrefix . $relativeTargetPathAndFilename;
        $uploadParameters = [
            'name' => $objectName,
            'predefinedAcl' => 'publicRead',
            'contentType' => $metaData->getMediaType(),
            'cacheControl' => 'public, max-age=1209600'
        ];

        if (in_array($metaData->getMediaType(), $this->gzipCompressionMediaTypes, true)) {
            try {
                $temporaryTargetPathAndFilename = $this->environment->getPathToTemporaryDirectory() . uniqid('Flownative_Google_CloudStorage_', true) . '.tmp';
                $temporaryTargetStream = gzopen($temporaryTargetPathAndFilename, 'wb' . $this->gzipCompressionLevel);
                while (!feof($sourceStream)) {
                    gzwrite($temporaryTargetStream, fread($sourceStream, 524288));
                }
                fclose($sourceStream);
                fclose($temporaryTargetStream);

                $sourceStream = fopen($temporaryTargetPathAndFilename, 'rb');
                $uploadParameters['metadata']['contentEncoding'] = 'gzip';

                $this->logger->debug(sprintf('Converted resource data of object "%s" in bucket "%s" with SHA1 hash "%s" to GZIP with level %s.', $objectName, $this->bucketName, $metaData->getSha1() ?: 'unknown', $this->gzipCompressionLevel), LogEnvironment::fromMethodName(__METHOD__));
            } catch (\Exception $e) {
                $this->messageCollector->append(sprintf('Failed publishing resource as object "%s" in bucket "%s" with SHA1 hash "%s": %s', $objectName, $this->bucketName, $metaData->getSha1() ?: 'unknown', $e->getMessage()), Error::SEVERITY_WARNING, 1520257344878);
            }
        }
        try {
            $this->getCurrentBucket()->upload($sourceStream, $uploadParameters);
            $this->logger->debug(sprintf('Successfully published resource as object "%s" in bucket "%s" with SHA1 hash "%s"', $objectName, $this->bucketName, $metaData->getSha1() ?: 'unknown'), LogEnvironment::fromMethodName(__METHOD__));
        } catch (\Exception $e) {
            $this->messageCollector->append(sprintf('Failed publishing resource as object "%s" in bucket "%s" with SHA1 hash "%s": %s', $objectName, $this->bucketName, $metaData->getSha1() ?: 'unknown', $e->getMessage()), Error::SEVERITY_WARNING, 1506847965352);
        } finally {
            if (is_resource($sourceStream)) {
                fclose($sourceStream);
            }
            if (isset($temporaryTargetPathAndFilename) && file_exists($temporaryTargetPathAndFilename)) {
                unlink($temporaryTargetPathAndFilename);
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
    protected function getRelativePublicationPathAndFilename(ResourceMetaDataInterface $object): string
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
    protected function encodeRelativePathAndFilenameForUri(string $relativePathAndFilename): string
    {
        return implode('/', array_map('rawurlencode', explode('/', $relativePathAndFilename)));
    }

    /**
     * @return Bucket
     */
    protected function getCurrentBucket(): Bucket
    {
        if ($this->currentBucket === null) {
            $this->currentBucket = $this->storageClient->bucket($this->bucketName);
        }
        return $this->currentBucket;
    }

    /**
     * Checks if the bucket and key prefix used as storage and target are the same
     *
     * @param CollectionInterface $collection
     * @return bool
     */
    protected function isOneBucketSetup(CollectionInterface $collection): bool
    {
        $storage = $collection->getStorage();
        return (
            $storage instanceof GcsStorage &&
            $storage->getBucketName() === $this->bucketName &&
            $storage->getKeyPrefix() === $this->keyPrefix
        );
    }
}

