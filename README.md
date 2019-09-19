[![MIT license](http://img.shields.io/badge/license-MIT-brightgreen.svg)](http://opensource.org/licenses/MIT)
[![Packagist](https://img.shields.io/packagist/v/flownative/google-cloudstorage.svg)](https://packagist.org/packages/flownative/google-cloudstorage)
[![Packagist](https://img.shields.io/packagist/dm/flownative/google-cloudstorage)](https://packagist.org/packages/flownative/google-cloudstorage)
[![Maintenance level: Love](https://img.shields.io/badge/maintenance-%E2%99%A1%E2%99%A1%E2%99%A1-ff69b4.svg)](https://www.flownative.com/en/products/open-source.html)

# Google Cloud Storage Adaptor for Neos 3.x/4.x and Flow 4.x/5.x

This [Flow](https://flow.neos.io) package allows you to store assets (resources) in [Google Cloud Storage](https://cloud.google.com/storage/)
and publish resources to GCS. Because [Neos CMS](https://www.neos.io) is using Flow's resource management under the hood,
this adaptor also works nicely for all kinds of assets in Neos.

## Key Features

- store all assets or only a specific collection in a private GCS bucket
- publish assets to a private or public GCS bucket
- supports GZIP compression for selected media types
- command line interface for basic tasks like connection check or emptying an GCS bucket

Using this connector, you can run a Neos website which does not store any asset (images, PDFs etc.) on your local webserver.

## Installation

The Flownative Google Cloud Storage connector is installed as a regular Flow package via Composer. For your existing
project, simply include `flownative/google-cloudstorage` into the dependencies of your Flow or Neos distribution:

For Neos 3.* and higher:

```bash
$ composer require flownative/google-cloudstorage:4.*
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
Neos:
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
Neos:
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

Some words regarding the configuration options:

The `keyPrefix` option allows you to share one bucket across multiple websites or applications. All object keys
will be prefixed by the given string.

The `baseUri` option defines the root of the publicly accessible address pointing to your published resources. In the
example above, baseUri points to a subdomain which needs to be set up separately. If `baseUri` is empty, the
Google Cloud Storage Publishing Target will determine a public URL automatically.

In order to copy the resources to the new storage we need a temporary collection that uses the storage and the new
publication target.

```yaml
Neos:
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
Neos:
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

## One- or Two-Bucket Setup

You can either create separate buckets for storage and target respectively or use the same bucket as storage
and target.

### One Bucket

In a one-bucket setup, the same bucket will be used as storage and target. All resources are publicly accessible,
so Flow can render a URL pointing to a resource right after it was uploaded.

This setup is fast and saves storage space, because resources do not have to be copied and are only stored once.
On the backside, the URLs are kind of ugly, because they only consist of a domain and the resource's SHA1:

```
https://storage.googleapis.com/bucket.example.com/a865defc2a48f060f15c3f4f21f2f1e78f154789
``` 

### Two Buckets

In a two-bucket setup, resources will be duplicated: the original is stored in the "storage" bucket and then
copied to the "target" bucket. Each time a new resource is created or imported, it will be stored in the
storage bucket and then automatically published (i.e. copied) into the target bucket.

You may choose this setup in order to have human- and SEO-friendly URLs pointing to your resources, because
objects copied into the target bucket can have a more telling name which includes the original filename of
the resource (see for the `publicPersistentResourceUris` options further below).

## Customizing the Public URLs

The Google Cloud Storage Target supports a way to customize the URLs which are presented to the user. Even
though the paths and filenames used for objects in the buckets is rather fixed (see above for the `baseUri` and
`keyPrefix` options), you may want to use a reverse proxy or content delivery network to deliver resources
stored in your target bucket. In that case, you can tell the Target to render URLs according to your own rules.
It is your responsibility then to make sure that these URLs actually work.

Let's assume that we have set up a webserver acting as a reverse proxy. Requests to `assets.flownative.com` are
re-written so that using a URI like `https://assets.flownative.com/a817…cb1/logo.svg` will actually deliver
a file stored in the Storage bucket using the given SHA1.

You can tell the Target to render URIs like these by defining a pattern with placeholders:

```yaml
      targets:
        googlePersistentResourcesTarget:
          target: 'Flownative\Google\CloudStorage\GcsTarget'
          targetOptions:
            bucket: 'flownativecom.flownative.cloud'
            baseUri: 'https://assets.flownative.com/'
            persistentResourceUris:
              pattern: '{baseUri}{sha1}/{filename}'
```

The possible placeholders are:

- `{baseUri}` The base URI as defined in the target options
- `{bucketName}` The target's bucket name
- `{keyPrefix}` The target's configured key prefix
- `{sha1}` The resource's SHA1
- `{md5}` The resource's MD5 🙄
- `{filename}` The resource's full filename, for example "logo.svg"
- `{fileExtension}` The resource's file extension, for example "svg"

The default pattern is: `https://storage.googleapis.com/{bucketName}/{keyPrefix}{sha1}`

## Publish Uris with Limited Lifetime

You can protect access to your resources by creating a private Google Cloud Storage bucket. For example, you
can declare a *bucket policy* which grants access only to a service key owned by your application.

Let's say you generate invoices as PDF files and want to store them securely in a private bucket. At some
point you will want allow authorized customers to download an invoice. The easiest way to implement that, is
to generate a special signed link, which allows access to a given resource for a limited time.

The Google Cloud Storage Target can take care of signing links to persistent resources. Just enable signing
and specify a signature lifetime (in seconds) like in the following example. Be aware though, that anyone with such a
generated link can download the given protected resource wile the link is valid. 

```yaml
      targets:
        googlePersistentResourcesTarget:
          target: 'Flownative\Google\CloudStorage\GcsTarget'
          targetOptions:
            bucket: 'flownativecom.flownative.cloud'
            baseUri: 'https://assets.flownative.com/'
            persistentResourceUris:
              pattern: '{baseUri}{sha1}/{filename}'
              enableSigning: true
              signatureLifetime: 600
```

## GZIP Compression

Google Cloud Storage supports GZIP compression for delivering files to the user, however, these files need to be
compressed outside Google Cloud Storage and then uploaded as GZIP compressed data. This plugin supports transcoding
resources on the fly, while they are being published. Data in the Google Cloud Storage *storage* is always
stored uncompressed, as-is. Files which is of one of the media types configured for GZIP compression are automatically
converted to GZIP while they are being published to the Google Cloud Storage *target*.

You can configure the compression level and the media types which should be compressed as such:

```yaml
Neos:
  Flow:
    resource:
      targets:
        googlePersistentResourcesTarget:
          target: 'Flownative\Google\CloudStorage\GcsTarget'
          targetOptions:
            gzipCompressionLevel: 9
            gzipCompressionMediaTypes:
            - 'text/plain'
            - 'text/css'
            - 'text/xml'
            - 'text/mathml'
            - 'text/javascript'
            - 'application/x-javascript'
            - 'application/xml'
            - 'application/rss+xml'
            - 'application/atom+xml'
            - 'application/javascript'
            - 'application/json'
            - 'application/x-font-woff'
            - 'image/svg+xml'
```

Note that adding media types for data which is already compressed – for example images or movies – will likely rather
increase the data size and thus should be avoided.

## Full Example Configuration for GCS

```yaml
Neos:
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
          target: 'Neos\Flow\ResourceManagement\Target\FileSystemTarget'
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
