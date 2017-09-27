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

use Google\Cloud\Core\ServiceBuilder;
use TYPO3\Flow\Annotations as Flow;

/**
 * Factory for the Google Cloud Storage service class
 *
 * @Flow\Scope("singleton")
 */
class StorageFactory
{
    /**
     * @Flow\InjectConfiguration("profiles")
     * @var array
     */
    protected $credentialProfiles;

    /**
     * @Flow\Inject
     * @var \TYPO3\Flow\Utility\Environment
     */
    protected $environment;

    /**
     * Creates a new Storage instance and authenticates agains the Google API
     *
     * @param string $credentialsProfileName
     * @return \Google\Cloud\Storage\StorageClient
     * @throws Exception
     */
    public function create($credentialsProfileName = 'default')
    {
        if (!isset($this->credentialProfiles[$credentialsProfileName])) {
            throw new Exception(sprintf('The specified Google Cloud Storage credentials profile "%s" does not exist, please check your settings.', $credentialsProfileName), 1446553024);
        }

        if (!empty($this->credentialProfiles[$credentialsProfileName]['credentials']['privateKeyJsonBase64Encoded'])) {
            $googleCloud = new ServiceBuilder([
                'keyFile' => json_decode(base64_decode($this->credentialProfiles[$credentialsProfileName]['credentials']['privateKeyJsonBase64Encoded']))
            ]);
        } else {
            if (substr($this->credentialProfiles[$credentialsProfileName]['credentials']['privateKeyJsonPathAndFilename'], 0, 1) !== '/') {
                $privateKeyPathAndFilename = FLOW_PATH_ROOT . $this->credentialProfiles[$credentialsProfileName]['credentials']['privateKeyJsonPathAndFilename'];
            } else {
                $privateKeyPathAndFilename = $this->credentialProfiles[$credentialsProfileName]['credentials']['privateKeyJsonPathAndFilename'];
            }

            if (!file_exists($privateKeyPathAndFilename)) {
                throw new Exception(sprintf('The Google Cloud Storage private key file "%s" does not exist. Either the file is missing or you need to adjust your settings.', $privateKeyPathAndFilename), 1446553054);
            }
            $googleCloud = new ServiceBuilder([
                'keyFilePath' => $privateKeyPathAndFilename
            ]);
        }

        return $googleCloud->storage();
    }
}
