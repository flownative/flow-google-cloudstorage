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

use Google_Client;
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
     * Creates a new Storage instance and authenticates agains the Google API
     *
     * @param string $credentialsProfileName
     * @return \Google_Service_Storage
     * @throws Exception
     */
    public function create($credentialsProfileName = 'default')
    {
        if (!isset($this->credentialProfiles[$credentialsProfileName])) {
            throw new Exception(sprintf('The specified Google Cloud Storage credentials profile "%s" does not exist, please check your settings.', $credentialsProfileName), 1446553024);
        }

        if (substr($this->credentialProfiles[$credentialsProfileName]['credentials']['privateKeyP12PathAndFilename'], 0, 1) !== '/') {
            $privateKeyPathAndFilename = FLOW_PATH_ROOT . $this->credentialProfiles[$credentialsProfileName]['credentials']['privateKeyP12PathAndFilename'];
        } else {
            $privateKeyPathAndFilename = $this->credentialProfiles[$credentialsProfileName]['credentials']['privateKeyP12PathAndFilename'];
        }

        if (!file_exists($privateKeyPathAndFilename)) {
            throw new Exception(sprintf('The Google Cloud Storage private key file "%s" does not exist. Either the file is missing or you need to adjust your settings.', $privateKeyPathAndFilename), 1446553054);
        }

        $privateKey = file_get_contents($privateKeyPathAndFilename);
        $credentials = new \Google_Auth_AssertionCredentials(
            $this->credentialProfiles[$credentialsProfileName]['credentials']['clientEmail'],
            [ \Google_Service_Storage::DEVSTORAGE_READ_WRITE ],
            $privateKey
        );

        $googleClient = new \Google_Client();
        $googleClient->setAssertionCredentials($credentials);
        if ($googleClient->isAccessTokenExpired()) {
            $googleClient->getRefreshToken();
        }

        return new \Google_Service_Storage($googleClient);
    }
}
