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
use TYPO3\Flow\Annotations as Flow;
use TYPO3\Flow\Cli\CommandController;

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
            $storageClient = $this->storageFactory->create();
        } catch (\Exception $e) {
            $this->outputLine($e->getMessage());
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
}
