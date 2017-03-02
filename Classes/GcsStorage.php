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
use Neos\Flow\ResourceManagement\CollectionInterface;
use Neos\Flow\ResourceManagement\PersistentResource;
use Neos\Flow\ResourceManagement\ResourceManager;
use Neos\Flow\ResourceManagement\ResourceRepository;
use Neos\Flow\ResourceManagement\Storage\Exception;
use Neos\Flow\ResourceManagement\Storage\StorageObject;
use Neos\Flow\ResourceManagement\Storage\WritableStorageInterface;
use Neos\Flow\Utility\Environment;

/**
 * A resource storage based on Google Cloud Storage
 */
class GcsStorage implements WritableStorageInterface
{

    /**
     * Name which identifies this resource storage
     *
     * @var string
     */
    protected $name;

    /**
     * Name of the bucket which should be used as a storage
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
     * @Flow\Inject
     * @var Environment
     */
    protected $environment;

    /**
     * @Flow\Inject
     * @var ResourceManager
     */
    protected $resourceManager;

    /**
     * @Flow\Inject
     * @var ResourceRepository
     */
    protected $resourceRepository;

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
     * @Flow\Inject
     * @var \Neos\Flow\Log\SystemLoggerInterface
     */
    protected $systemLogger;

    /**
     * Constructor
     *
     * @param string $name Name of this storage instance, according to the resource settings
     * @param array $options Options for this storage
     * @throws Exception
     */
    public function __construct($name, array $options = array())
    {
        $this->name = $name;
        $this->bucketName = $name;
        foreach ($options as $key => $value) {
            switch ($key) {
                case 'bucket':
                    $this->bucketName = $value;
                break;
                case 'keyPrefix':
                    $this->keyPrefix = ltrim($value, '/');
                break;
                default:
                    if ($value !== null) {
                        throw new Exception(sprintf('An unknown option "%s" was specified in the configuration of the "%s" resource GcsStorage. Please check your settings.', $key, $name), 1446667391);
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
     * Returns the instance name of this storage
     *
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * Returns the bucket name used as a storage
     *
     * @return string
     */
    public function getBucketName()
    {
        return $this->bucketName;
    }

    /**
     * Returns the object key prefix
     *
     * @return string
     */
    public function getKeyPrefix()
    {
        return $this->keyPrefix;
    }

    /**
     * Imports a resource (file) from the given URI or PHP resource stream into this storage.
     *
     * On a successful import this method returns a PersistentResource object representing the newly
     * imported persistent resource.
     *
     * @param string | resource $source The URI (or local path and filename) or the PHP resource stream to import the resource from
     * @param string $collectionName Name of the collection the new PersistentResource belongs to
     * @return PersistentResource A resource object representing the imported resource
     * @throws \Neos\Flow\ResourceManagement\Storage\Exception
     */
    public function importResource($source, $collectionName)
    {
        $temporaryTargetPathAndFilename = $this->environment->getPathToTemporaryDirectory() . uniqid('Flownative_Google_CloudStorage_');

        if (is_resource($source)) {
            try {
                $target = fopen($temporaryTargetPathAndFilename, 'wb');
                stream_copy_to_stream($source, $target);
                fclose($target);
            } catch (\Exception $e) {
                throw new Exception(sprintf('Could import the content stream to temporary file "%s".', $temporaryTargetPathAndFilename), 1446667392);
            }
        } else {
            try {
                copy($source, $temporaryTargetPathAndFilename);
            } catch (\Exception $e) {
                throw new Exception(sprintf('Could not copy the file from "%s" to temporary file "%s".', $source, $temporaryTargetPathAndFilename), 1446667394);
            }
        }

        $resource = $this->importTemporaryFile($temporaryTargetPathAndFilename, $collectionName);
        unlink($temporaryTargetPathAndFilename);

        return $resource;
    }

    /**
     * Imports a resource from the given string content into this storage.
     *
     * On a successful import this method returns a PersistentResource object representing the newly
     * imported persistent resource.
     *
     * The specified filename will be used when presenting the resource to a user. Its file extension is
     * important because the resource management will derive the IANA Media Type from it.
     *
     * @param string $content The actual content to import
     * @param string $collectionName Name of the collection the new PersistentResource belongs to
     * @return PersistentResource A resource object representing the imported resource
     * @throws Exception
     * @api
     */
    public function importResourceFromContent($content, $collectionName)
    {
        $sha1Hash = sha1($content);
        $md5Hash = md5($content);
        $filename = $sha1Hash;

        $resource = new PersistentResource();
        $resource->setFilename($filename);
        $resource->setFileSize(strlen($content));
        $resource->setCollectionName($collectionName);
        $resource->setSha1($sha1Hash);
        $resource->setMd5($md5Hash);

        $storageObject = new \Google_Service_Storage_StorageObject();
        $storageObject->setBucket($this->bucketName);
        $storageObject->setName($this->keyPrefix . $sha1Hash);
        $storageObject->setSize($resource->getFileSize());

        $this->storageService->objects->insert($this->bucketName, $storageObject, ['data' => $content, 'uploadType' => 'media']);

        return $resource;
    }

    /**
     * Imports a resource (file) as specified in the given upload info array as a
     * persistent resource.
     *
     * On a successful import this method returns a PersistentResource object representing
     * the newly imported persistent resource.
     *
     * @param array $uploadInfo An array detailing the resource to import (expected keys: name, tmp_name)
     * @param string $collectionName Name of the collection this uploaded resource should be part of
     * @return string A resource object representing the imported resource
     * @throws Exception
     * @api
     */
    public function importUploadedResource(array $uploadInfo, $collectionName)
    {
        $pathInfo = pathinfo($uploadInfo['name']);
        $originalFilename = $pathInfo['basename'];
        $sourcePathAndFilename = $uploadInfo['tmp_name'];

        if (!file_exists($sourcePathAndFilename)) {
            throw new Exception(sprintf('The temporary file "%s" of the file upload does not exist (anymore).', $sourcePathAndFilename), 1446667850);
        }

        $newSourcePathAndFilename = $this->environment->getPathToTemporaryDirectory() . 'Flownative_Google_CloudStorage_' . uniqid() . '.tmp';
        if (move_uploaded_file($sourcePathAndFilename, $newSourcePathAndFilename) === false) {
            throw new Exception(sprintf('The uploaded file "%s" could not be moved to the temporary location "%s".', $sourcePathAndFilename, $newSourcePathAndFilename), 1446667851);
        }

        $sha1Hash = sha1_file($newSourcePathAndFilename);
        $md5Hash = md5_file($newSourcePathAndFilename);

        $resource = new PersistentResource();
        $resource->setFilename($originalFilename);
        $resource->setCollectionName($collectionName);
        $resource->setFileSize(filesize($newSourcePathAndFilename));
        $resource->setSha1($sha1Hash);
        $resource->setMd5($md5Hash);

        $this->insertIntoBucket($this->bucketName, $this->keyPrefix . $sha1Hash, $newSourcePathAndFilename);

        return $resource;
    }

    /**
     * Deletes the storage data related to the given PersistentResource object
     *
     * @param PersistentResource $resource The PersistentResource to delete the storage data of
     * @return bool TRUE if removal was successful
     * @throws \Exception
     * @throws \Google_Service_Exception
     * @api
     */
    public function deleteResource(PersistentResource $resource)
    {
        try {
            $this->storageService->objects->delete($this->bucketName, $this->keyPrefix . $resource->getSha1());
        } catch (\Google_Service_Exception $e) {
            if ($e->getCode() === 404) {
                return true;
            }
            throw $e;
        }

        return true;
    }

    /**
     * Returns a stream handle which can be used internally to open / copy the given resource
     * stored in this storage.
     *
     * @param PersistentResource $resource The resource stored in this storage
     * @return bool|resource A URI (for example the full path and filename) leading to the resource file or FALSE if it does not exist
     * @throws Exception
     * @api
     */
    public function getStreamByResource(PersistentResource $resource)
    {
        try {
            $storageObject = $this->storageService->objects->get($this->bucketName, $this->keyPrefix . $resource->getSha1(), ['alt' => 'media']);
            $this->systemLogger->log('Create tmpfile for ' . $resource->getSha1());
            $fh = tmpfile();
            fwrite($fh, $storageObject);
            rewind($fh);

            return $fh;
        } catch (\Exception $e) {
            if ($e instanceof \Google_Service_Exception && $e->getCode() === 404) {
                return false;
            }
            $message = sprintf('Could not retrieve stream for resource %s (/%s/%s%s). %s', $resource->getFilename(), $this->bucketName, $this->keyPrefix . $resource->getSha1(), $e->getMessage());
            $this->systemLogger->log($message, \LOG_ERR);
            throw new Exception($message, 1446667860);
        }
    }

    /**
     * Returns a stream handle which can be used internally to open / copy the given resource
     * stored in this storage.
     *
     * @param string $relativePath A path relative to the storage root, for example "MyFirstDirectory/SecondDirectory/Foo.css"
     * @return bool|resource A URI (for example the full path and filename) leading to the resource file or FALSE if it does not exist
     * @throws Exception
     * @api
     */
    public function getStreamByResourcePath($relativePath)
    {
        try {
            $storageObject = $this->storageService->objects->get($this->bucketName, $this->keyPrefix . ltrim($relativePath, '/'), ['alt' => 'media']);
            $this->systemLogger->log('Create tmpfile for ' . $relativePath);
            $fh = tmpfile();
            fwrite($fh, $storageObject);
            rewind($fh);

            return $fh;
        } catch (\Exception $e) {
            if ($e instanceof \Google_Service_Exception && $e->getCode() === 404) {
                return false;
            }
            $message = sprintf('Could not retrieve stream for resource (gs://%s/%s). %s', $this->bucketName, $this->keyPrefix . ltrim($relativePath, '/'), $e->getMessage());
            $this->systemLogger->log($message, \LOG_ERR);
            throw new Exception($message, 1446667861);
        }
    }

    /**
     * Retrieve all Objects stored in this storage.
     *
     * @return array<\Neos\Flow\ResourceManagement\Storage\StorageObject>
     * @api
     */
    public function getObjects()
    {
        $objects = array();
        foreach ($this->resourceManager->getCollectionsByStorage($this) as $collection) {
            $objects = array_merge($objects, $this->getObjectsByCollection($collection));
        }

        return $objects;
    }

    /**
     * Retrieve all Objects stored in this storage, filtered by the given collection name
     *
     * @param CollectionInterface $collection
     * @return array<\Neos\Flow\ResourceManagement\Storage\StorageObject>
     * @api
     */
    public function getObjectsByCollection(CollectionInterface $collection)
    {
        $objects = array();
        $that = $this;
        $bucketName = $this->bucketName;
        $keyPrefix = $this->keyPrefix;
        $storageService = $this->storageService;

        foreach ($this->resourceRepository->findByCollectionName($collection->getName()) as $resource) {
            /** @var \Neos\Flow\ResourceManagement\PersistentResource $resource */
            $object = new StorageObject();
            $object->setFilename($resource->getFilename());
            $object->setSha1($resource->getSha1());
            $object->setStream(function () use ($that, $bucketName, $keyPrefix, $storageService, $resource) {
                $storageObject = $storageService->objects->get($bucketName, $keyPrefix . $resource->getSha1(), ['alt' => 'media']);
                $fh = fopen('php://temp', 'w+');
                fwrite($fh, $storageObject);
                rewind($fh);

                return $fh;
            });
            $objects[] = $object;
        }

        return $objects;
    }

    /**
     * Imports the given temporary file into the storage and creates the new resource object.
     *
     * @param string $temporaryPathAndFilename Path and filename leading to the temporary file
     * @param string $collectionName Name of the collection to import into
     * @return PersistentResource The imported resource
     * @throws \Exception
     * @throws \Google_Service_Exception
     */
    protected function importTemporaryFile($temporaryPathAndFilename, $collectionName)
    {
        $sha1Hash = sha1_file($temporaryPathAndFilename);
        $md5Hash = md5_file($temporaryPathAndFilename);

        $resource = new PersistentResource();
        $resource->setFileSize(filesize($temporaryPathAndFilename));
        $resource->setCollectionName($collectionName);
        $resource->setSha1($sha1Hash);
        $resource->setMd5($md5Hash);

        try {
            $this->storageService->objects->get($this->bucketName, $this->keyPrefix . $sha1Hash);
            $alreadyExists = true;
        } catch (\Google_Service_Exception $e) {
            if ($e->getCode() === 404) {
                $alreadyExists = false;
            } else {
                throw $e;
            }
        }

        if (!$alreadyExists) {
            $this->insertIntoBucket($this->bucketName, $this->keyPrefix . $sha1Hash, $temporaryPathAndFilename);

            $this->systemLogger->log(sprintf('Successfully imported resource as object "%s" into bucket "%s" with MD5 hash "%s"', $sha1Hash, $this->bucketName, $resource->getMd5() ?: 'unknown'), LOG_INFO);
        } else {
            $this->systemLogger->log(sprintf('Did not import resource as object "%s" into bucket "%s" because that object already existed.', $sha1Hash, $this->bucketName), LOG_INFO);
        }

        return $resource;
    }

    /**
     * Insert the contents of $pathAndFilename into the given $bucketName as $name.
     *
     * @param string $bucketName
     * @param string $name
     * @param string $pathAndFilename
     * @return void
     */
    protected function insertIntoBucket($bucketName, $name, $pathAndFilename)
    {
        $chunkSizeBytes = 100 * 1024 * 1024;

        $storageObject = new \Google_Service_Storage_StorageObject();
        $storageObject->setBucket($bucketName);
        $storageObject->setName($name);
        $storageObject->setSize(filesize($pathAndFilename));

        $this->storageService->getClient()->setDefer(true);
        $request = $this->storageService->objects->insert($bucketName, $storageObject, ['name' => $name, 'uploadType' => 'resumable']);

        // Create a media file upload to represent our upload process.
        $media = new \Google_Http_MediaFileUpload(
            $this->storageService->getClient(),
            $request,
            'text/plain',
            null,
            true,
            $chunkSizeBytes
        );
        $media->setFileSize(filesize($pathAndFilename));

        $status = false;
        $handle = fopen($pathAndFilename, 'rb');
        while (!$status && !feof($handle)) {
            $chunk = fread($handle, $chunkSizeBytes);
            $status = $media->nextChunk($chunk);
        }
        fclose($handle);

        $this->storageService->getClient()->setDefer(false);
    }
}
