# content-mapping-destinationadapter-solarium #

DestinationAdapter for the solarium Solr client inside the [webfactory/content-mapping](https://github.com/webfactory/content-mapping)
mini framework.


## Installation ##

    composer require webfactory/content-mapping-destinationadapter-solarium


## Usage ##

```php
use Solarium\Client;
use Webfactory\ContentMapping\Synchronizer;
use Webfactory\ContentMapping\Solr\SolariumDestinationAdapter;

$solrClient = new Client($configArray); // see solarium documentation for details
$logger = ...; // any PSR3-Logger

$destinationAdapter = new SolariumDestinationAdapter($solrClient, $logger);

$synchronizer = new Synchronizer($sourceAdapter, $mapper, $destinationAdapter, $logger);
```


## Credits, Copyright and License ##

This project was started at webfactory GmbH, Bonn.

- <http://www.webfactory.de>
- <http://twitter.com/webfactory>

Copyright 2015 webfactory GmbH, Bonn. Code released under [the MIT license](LICENSE).
