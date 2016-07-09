# content-mapping-adapter-solarium #

[![Build Status](https://travis-ci.org/h69/content-mapping-adapter-solarium.svg?branch=master)](https://travis-ci.org/h69/content-mapping-adapter-solarium)
[![Coverage Status](https://coveralls.io/repos/github/h69/content-mapping-adapter-solarium/badge.svg?branch=master)](https://coveralls.io/github/h69/content-mapping-adapter-solarium?branch=master)

Adapter for the solarium Solr client inside the [h69/content-mapping](https://github.com/h69/content-mapping) mini framework.


## Installation ##

    composer require h69/content-mapping-adapter-solarium


## Usage ##

```php
use Solarium\Client as SolariumClient;
use H69\ContentMapping\Synchronizer;
use H69\ContentMapping\Solarium\Adapter as SolariumAdapter;

$sourceAdapter = ...;
$destinationAdapter = new SolariumAdapter(new SolariumClient($configArray));
$typeToSynchronize = 'pages';

$synchronizer = new Synchronizer($sourceAdapter, $destinationAdapter);
$synchronizer->synchronize($typeToSynchronize, function($objectA, $objectB){
    ...
    //return Result::unchanged();
    return Result::changed($updatedObjectB);
});
```


## Credits, Copyright and License ##

This project/copy was started at [webfactory GmbH, Bonn](https://www.webfactory.de) and was/will be further developed by
- [h69](https://github.com/h69)

Copyright 2016. Code released under [the MIT license](LICENSE).
