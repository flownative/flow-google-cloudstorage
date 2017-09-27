[![MIT license](http://img.shields.io/badge/license-MIT-brightgreen.svg)](http://opensource.org/licenses/MIT)
![Packagist][packagist]

[packagist]: https://img.shields.io/packagist/v/flownative/google-cloudstorage.svg

# Google Cloud Storage Adaptor for Neos 2.x and Flow 3.x

This [Flow](https://flow.neos.io) package allows you to store assets (resources) in [Google Cloud Storage](https://cloud.google.com/storage/)
and publish resources to GCS. Because [Neos CMS](https://www.neos.io) is using Flow's resource management under the hood,
this adaptor also works nicely for all kinds of assets in Neos.

NOTE: This package is in beta stage. It works fine for some, but there is little real-world experience with it yet.

## Key Features

- store all assets or only a specific collection in a private GCS bucket
- publish assets to a private or public GCS bucket
- command line interface for basic tasks like connection check or emptying an GCS bucket

Using this connector, you can run a Neos website which does not store any asset (images, PDFs etc.) on your local webserver.

## Installation

The Flownative Google Cloud Storage connector is installed as a regular Flow package via Composer. For your existing
project, simply include `flownative/google-cloudstorage` into the dependencies of your Flow or Neos distribution:

```bash
$ composer require flownative/google-cloudstorage:~1.0@beta
```

## Configuration

In order to communicate with the Google API, you need to provide the credentials of an account which has access
to GCS (see next section for instructions for setting up the service user). Add the following configuration to the
`Settings.yaml` for your desired Flow context (for example in `Configuration/Production/Settings.yaml`) and make sure
to replace client email and the private key with your own data:
  
```yaml
Flownative:
  Google:
    CloudStorage:
      profiles:
        default:
          credentials:
            clientEmail: '123456789012-abc123defg456hijklmnopqrstuvwxyz@developer.gserviceaccount.com'
            privateKeyJsonPathAndFilename: 'Data/Secrets/MyGoogleProject-abc123457def.json'
```

Instead of using a file, the private key can also be specified directly, as a base64-encoded string. This allows for
providing the private key via an environment variable:

```yaml
Flownative:
  Google:
    CloudStorage:
      profiles:
        default:
          credentials:
            clientEmail: '123456789012-abc123defg456hijklmnopqrstuvwxyz@developer.gserviceaccount.com'
            privateKeyJsonBase64Encoded: '%env:SOME_ENVIRONMENT_VARIABLE_WITH_PRIVATE_KEY%'
```

You can test your settings by executing the `connect` command with a bucket of your choice.

```bash
$ ./flow gcs:connect storage.example.net
```

Right now, you can only define one connection profile, namely the "default" profile. Additional profiles may be supported
in future versions.

## User Setup

tbd.

## Publish Assets to Google Cloud Storage

Once the connector package is in place, you add a new publishing target which uses that connect and assign this target
to your collection.

```yaml
TYPO3:
  Flow:
    resource:
      collections:
        persistent:
          target: 'googlePersistentResourcesTarget'
      targets:
        googlePersistentResourcesTarget:
          target: 'Flownative\Google\CloudStorage\GcsTarget'
          targetOptions:
            bucket: 'target.example.net'
            keyPrefix: '/'
            baseUri: 'http://storage.googleapis.com/target.example.net/'
```

Since the new publishing target will be empty initially, you need to publish your assets to the new target by using the
``resource:publish`` command:

```bash
$ ./flow resource:publish
```

This command will upload your files to the target and use the calculated remote URL for all your assets from now on.

## Switching the Storage of a Collection

If you want to migrate from your default local filesystem storage to a remote storage, you need to copy all your existing
persistent resources to that new storage and use that storage afterwards by default.

You start by adding a new storage with the GCS connector to your configuration. As you might want also want to serve your
assets by the remote storage system, you also add a target that contains your published resources.

```yaml
TYPO3:
  Flow:
    resource:
      storages:
        googlePersistentResourcesStorage:
          storage: 'Flownative\Google\CloudStorage\GcsStorage'
          storageOptions:
            bucket: 'storage.example.net'
            keyPrefix: '/'
      targets:
        googlePersistentResourcesTarget:
          target: 'Flownative\Google\CloudStorage\GcsTarget'
          targetOptions:
            bucket: 'target.example.net'
            keyPrefix: '/'
            baseUri: 'http://storage.googleapis.com/target.example.net/'
```

Some notes regarding the configuration:

You should create separate buckets for storage and target respectively, because the storage will remain private and the
target will potentially be published. Even if it might work using one bucket for both, this is not recommended.

The `keyPrefix` option allows you to share one bucket across multiple websites or applications. All object keys
will be prefixed by the given string.

The `baseUri` option defines the root of the publicly accessible address pointing to your published resources. In the
example above, baseUri points to a subdomain which needs to be set up separately. If `baseUri` is empty, the
Google Cloud Storage Publishing Target will determine a public URL automatically.

In order to copy the resources to the new storage we need a temporary collection that uses the storage and the new
publication target.

```yaml
TYPO3:
  Flow:
    resource:
      collections:
        tmpNewCollection:
          storage: 'googlePersistentResourcesStorage'
          target: 'googlePersistentResourcesTarget'
```

Now you can use the ``resource:copy`` command (available in Flow 3.1 or Neos 2.1 and higher):

```bash
$ ./flow resource:copy --publish persistent tmpNewCollection
```

This will copy all your files from your current storage (local filesystem) to the new remote storage. The ``--publish``
flag means that this command also publishes all the resources to the new target, and you have the same state on your
current storage and publication target as on the new one.

Now you can overwrite your old collection configuration and remove the temporary one:

```yaml
TYPO3:
  Flow:
    resource:
      collections:
        persistent:
          storage: 'googlePersistentResourcesStorage'
          target: 'googlePersistentResourcesTarget'
```

Clear caches and you're done.

```bash
$ ./flow flow:cache:flush
```

## Full Example Configuration for GCS

```yaml
TYPO3:
  Flow:
    resource:
      storages:
        googlePersistentResourcesStorage:
          storage: 'Flownative\Google\CloudStorage\GcsStorage'
          storageOptions:
            bucket: 'storage.example.net'
            keyPrefix: '/'
      collections:
        # Collection which contains all persistent resources
        persistent:
          storage: 'googlePersistentResourcesStorage'
          target: 'googlePersistentResourcesTarget'
      targets:
        localWebDirectoryPersistentResourcesTarget:
          target: 'TYPO3\Flow\Resource\Target\FileSystemTarget'
          targetOptions:
            path: '%FLOW_PATH_WEB%_Resources/Persistent/'
            baseUri: '_Resources/Persistent/'
            subdivideHashPathSegment: false
        googlePersistentResourcesTarget:
          target: 'Flownative\Google\CloudStorage\GcsTarget'
          targetOptions:
            bucket: 'target.example.net'
            keyPrefix: '/'
            baseUri: 'http://storage.googleapis.com/target.example.net/'

Flownative:
  Google:
    CloudStorage:
      profiles:
        default:
          credentials:
            clientEmail: '123456789012-abc123defg456hijklmnopqrstuvwxyz@developer.gserviceaccount.com'
            privateKeyJsonPathAndFilename: 'Data/Secrets/MyGoogleProject-abc123457def.json'
```
