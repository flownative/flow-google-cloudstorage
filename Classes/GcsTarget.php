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

use Neos\Flow\Annotations as Flow;
use Neos\Flow\Log\SystemLoggerInterface;
use Neos\Flow\ResourceManagement\CollectionInterface;
use Neos\Flow\ResourceManagement\Exception;
use Neos\Flow\ResourceManagement\PersistentResource;
use Neos\Flow\ResourceManagement\Publishing\MessageCollector;
use Neos\Flow\ResourceManagement\ResourceManager;
use Neos\Flow\ResourceManagement\ResourceMetaDataInterface;
use Neos\Flow\ResourceManagement\Target\TargetInterface;

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
     * @var StorageFactory
     */
    protected $storageFactory;

    /**
     * @var \Google_Service_Storage
     */
    protected $storageService;

    /**
     * @var array
     */
    protected $existingObjectsInfo;

	/**
	 * @Flow\Inject
	 * @var MessageCollector
	 */
	protected $messageCollector;

	/**
	 * @Flow\InjectConfiguration("continuesFailureLimit")
	 * @var int
	 */
	protected $continuesFailureLimit;

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
     */
    public function initializeObject()
    {
        $this->storageService = $this->storageFactory->create();
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
     * Publishes the whole collection to this target
     *
     * @param CollectionInterface $collection The collection to publish
     * @throws Exception
     * @throws \Exception
     * @throws \Google_Service_Exception
     */
    public function publishCollection(CollectionInterface $collection)
    {
        if (!isset($this->existingObjectsInfo)) {
            $this->existingObjectsInfo = [];
            $parameters = [
                'prefix' => $this->keyPrefix
            ];

            do {
                $storageObjects = $this->storageService->objects->listObjects($this->bucketName, $parameters);
                foreach ($storageObjects->getItems() as $storageObject) {
                    /** @var \Google_Service_Storage_StorageObject $storageObject */
                    $this->existingObjectsInfo[$storageObject->getName()] = true;
                }
                $pageToken = $storageObjects->getNextPageToken();
                $parameters['pageToken'] = $pageToken;
            } while ($pageToken !== null);
        }

        $obsoleteObjects = $this->existingObjectsInfo;

        $storage = $collection->getStorage();
        if ($storage instanceof GcsStorage) {
            $storageBucketName = $storage->getBucketName();
            if ($storageBucketName === $this->bucketName && $storage->getKeyPrefix() === $this->keyPrefix) {
                throw new Exception(sprintf('Could not publish collection %s because the source and target bucket is the same, with identical key prefixes. Either choose a different bucket or at least key prefix for the target.', $collection->getName()), 1446721125);
            }
           $failureCounter = 0;
            foreach ($collection->getObjects() as $object) {
                /** @var \Neos\Flow\ResourceManagement\Storage\StorageObject $object */
                $objectName = $this->keyPrefix . $this->getRelativePublicationPathAndFilename($object);

                $storageObject = new \Google_Service_Storage_StorageObject();
                $storageObject->setBucket($this->bucketName);
                $storageObject->setName($objectName);
                $storageObject->setContentType($object->getMediaType());
                $storageObject->setSize($object->getFileSize());
                $storageObject->setCacheControl('public, max-age=1209600');

                $parameters = [
                    'destinationPredefinedAcl' => 'publicRead',
                    'mimeType' => $object->getMediaType()
                ];
                try {
                    $this->storageService->objects->copy($storageBucketName, $storage->getKeyPrefix() . $object->getSha1(), $this->bucketName, $objectName, $storageObject, $parameters);
                    $failureCounter = 0;
                } catch (\Google_Service_Exception $e) {
                    if($this->continuesFailureLimit == $failureCounter ){
                       throw new Exception(sprintf('There where more then %s continues failures and the collection "%s" stopped to publish further resources', $failureCounter, $collection->getName()), 1501586031);
                    }
                    $failureCounter ++;
                    $this->messageCollector->append(sprintf('Could not copy resource with SHA1 hash %s of collection %s from bucket %s to %s: %s', $object->getSha1(), $collection->getName(), $storageBucketName, $this->bucketName, $e->getMessage()));;
                    unset($obsoleteObjects[$this->getRelativePublicationPathAndFilename($object)]);
                    continue;
                }
                $this->systemLogger->log(sprintf('Successfully copied resource as object "%s" (MD5: %s) from bucket "%s" to bucket "%s"', $objectName, $object->getMd5() ?: 'unknown', $storageBucketName, $this->bucketName), LOG_DEBUG);
                unset($obsoleteObjects[$this->getRelativePublicationPathAndFilename($object)]);
            }
        } else {
            foreach ($collection->getObjects() as $object) {
                /** @var \Neos\Flow\ResourceManagement\Storage\StorageObject $object */
                $this->publishFile($object->getStream(), $this->getRelativePublicationPathAndFilename($object), $object);
                unset($obsoleteObjects[$this->getRelativePublicationPathAndFilename($object)]);
            }
        }

        foreach (array_keys($obsoleteObjects) as $relativePathAndFilename) {
            try {
                $this->storageService->objects->delete($this->bucketName, $this->keyPrefix . $relativePathAndFilename);
            } catch (\Google_Service_Exception $e) {
                if ($e->getCode() !== 404) {
                    throw $e;
                }
            }
        }
    }

    /**
     * Returns the web accessible URI pointing to the given static resource
     *
     * @param string $relativePathAndFilename Relative path and filename of the static resource
     * @return string The URI
     */
    public function getPublicStaticResourceUri($relativePathAndFilename)
    {
        return $this->storageService->objects->get($this->bucketName, $this->keyPrefix . $relativePathAndFilename)->getMediaLink();
    }

    /**
     * Publishes the given persistent resource from the given storage
     *
     * @param \Neos\Flow\ResourceManagement\PersistentResource $resource The resource to publish
     * @param CollectionInterface $collection The collection the given resource belongs to
     * @return void
     * @throws Exception
     */
    public function publishResource(PersistentResource $resource, CollectionInterface $collection)
    {
        $storage = $collection->getStorage();
        if ($storage instanceof GcsStorage) {
            if ($storage->getBucketName() === $this->bucketName && $storage->getKeyPrefix() === $this->keyPrefix) {
                throw new Exception(sprintf('Could not publish resource with SHA1 hash %s of collection %s because the source and target bucket is the same, with identical key prefixes. Either choose a different bucket or at least key prefix for the target.', $resource->getSha1(), $collection->getName()), 1446721574);
            }
            $objectName = $this->keyPrefix . $this->getRelativePublicationPathAndFilename($resource);

            $storageObject = new \Google_Service_Storage_StorageObject();
            $storageObject->setBucket($this->bucketName);
            $storageObject->setName($objectName);
            $storageObject->setContentType($resource->getMediaType());
            $storageObject->setSize($resource->getFileSize());
            $storageObject->setCacheControl('public, max-age=1209600');

            $parameters = [
                'destinationPredefinedAcl' => 'publicRead',
                'mimeType' => $resource->getMediaType()
            ];
            try {
                $this->storageService->objects->copy($storage->getBucketName(), $storage->getKeyPrefix() . $resource->getSha1(), $this->bucketName, $objectName, $storageObject, $parameters);
            } catch (\Google_Service_Exception $e) {
                throw new Exception(sprintf('Could not copy resource with SHA1 hash %s of collection %s from bucket %s to %s: %s', $resource->getSha1(), $collection->getName(), $storage->getBucketName(), $this->bucketName, $e->getMessage()), 1446721791);
            }

            $this->systemLogger->log(sprintf('Successfully published resource as object "%s" (MD5: %s) by copying from bucket "%s" to bucket "%s"', $objectName, $resource->getMd5() ?: 'unknown', $storage->getBucketName(), $this->bucketName), LOG_DEBUG);
        } else {
            $sourceStream = $resource->getStream();
            if ($sourceStream === false) {
                throw new Exception(sprintf('Could not publish resource with SHA1 hash %s of collection %s because there seems to be no corresponding data in the storage.', $resource->getSha1(), $collection->getName()), 1446721810);
            }
            $this->publishFile($sourceStream, $this->getRelativePublicationPathAndFilename($resource), $resource);
        }
    }

    /**
     * Unpublishes the given persistent resource
     *
     * @param \Neos\Flow\ResourceManagement\PersistentResource $resource The resource to unpublish
     * @throws \Exception
     * @throws \Google_Service_Exception
     */
    public function unpublishResource(PersistentResource $resource)
    {
        try {
            $objectName = $this->keyPrefix . $this->getRelativePublicationPathAndFilename($resource);
            $this->storageService->objects->delete($this->bucketName, $objectName);
            $this->systemLogger->log(sprintf('Successfully unpublished resource as object "%s" (MD5: %s) from bucket "%s"', $objectName, $resource->getMd5() ?: 'unknown', $this->bucketName), LOG_DEBUG);
        } catch (\Google_Service_Exception $e) {
            if ($e->getCode() !== 404) {
                throw $e;
            }
        }
    }

    /**
     * Returns the web accessible URI pointing to the specified persistent resource
     *
     * @param \Neos\Flow\ResourceManagement\PersistentResource $resource PersistentResource object or the resource hash of the resource
     * @return string The URI
     * @throws Exception
     */
    public function getPublicPersistentResourceUri(PersistentResource $resource)
    {
        if ($this->baseUri != '') {
            return $this->baseUri . $this->getRelativePublicationPathAndFilename($resource);
        } else {
            return $this->storageService->objects->get($this->bucketName, $this->keyPrefix . $this->getRelativePublicationPathAndFilename($resource))->getMediaLink();
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

        $storageObject = new \Google_Service_Storage_StorageObject();
        $storageObject->setBucket($this->bucketName);
        $storageObject->setName($objectName);
        $storageObject->setContentType($metaData->getMediaType());
        $storageObject->setSize($metaData->getFileSize());
        $storageObject->setCacheControl('public, max-age=1209600');

        try {
            $parameters = [
                'data' => stream_get_contents($sourceStream),
                'uploadType' => 'media',
                'predefinedAcl' => 'publicRead',
                'mimeType' => $metaData->getMediaType()
            ];

            $this->storageService->objects->insert($this->bucketName, $storageObject, $parameters);
            $this->systemLogger->log(sprintf('Successfully published resource as object "%s" in bucket "%s" with MD5 hash "%s"', $objectName, $this->bucketName, $metaData->getMd5() ?: 'unknown'), LOG_DEBUG);
        } catch (\Exception $e) {
            $this->systemLogger->log(sprintf('Failed publishing resource as object "%s" in bucket "%s" with MD5 hash "%s": %s', $objectName, $this->bucketName, $metaData->getMd5() ?: 'unknown', $e->getMessage()), LOG_DEBUG);
            if (is_resource($sourceStream)) {
                fclose($sourceStream);
            }
            throw $e;
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
        return $this->encodeRelativePathAndFilenameForUri($pathAndFilename);
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

}

