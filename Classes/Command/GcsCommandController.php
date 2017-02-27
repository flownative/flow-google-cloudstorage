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

use Flownative\Google\CloudStorage\StorageFactory;
use Neos\Flow\Annotations as Flow;
use Neos\Flow\Cli\CommandController;

/**
 * Google Cloud Storage command controller
 *
 * @Flow\Scope("singleton")
 */
class GcsCommandController extends CommandController
{
    /**
     * @Flow\Inject
     * @var StorageFactory
     */
    protected $storageFactory;

    /**
     * Checks the connection
     *
     * This command checks if the configured credentials and connectivity allows for connecting with the Google API.
     *
     * @param string $bucket The bucket which is used for trying to upload and retrieve some test data
     * @return void
     */
    public function connectCommand($bucket)
    {
        try {
            $storageService = $this->storageFactory->create();
        } catch (\Exception $e) {
            $this->outputLine($e->getMessage());
            exit(1);
        }

        $storageObject = new \Google_Service_Storage_StorageObject();
        $storageObject->setName('Flownative.Google.CloudStorage.ConnectionTest.txt');
        $storageObject->setSize(4);

        $postBody = [
            'data' => 'test',
            'uploadType' => 'media'
        ];

        $this->outputLine('Writing test object into bucket (%s) ...', [$bucket]);
        $storageService->objects->insert($bucket, $storageObject, $postBody);

        $this->outputLine('Retrieving test object from bucket ...');
        $storageService->objects->get($bucket, 'Flownative.Google.CloudStorage.ConnectionTest.txt');

        $this->outputLine('Deleting test object from bucket ...');
        $storageService->objects->delete($bucket, 'Flownative.Google.CloudStorage.ConnectionTest.txt');

        $this->outputLine('OK');
    }
}
