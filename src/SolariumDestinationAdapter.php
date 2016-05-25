<?php
/*
 * (c) webfactory GmbH <info@webfactory.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Webfactory\ContentMapping\DestinationAdapter\Solarium;

use Psr\Log\LoggerInterface;
use Solarium\Client;
use Solarium\QueryType\Select\Result\DocumentInterface as SelectedDocumentInterface;
use Solarium\QueryType\Select\Result\Result as SelectResult;
use Solarium\QueryType\Update\Query\Document\DocumentInterface;
use Webfactory\ContentMapping\DestinationAdapter;
use Solarium\QueryType\Update\Query\Document\Document;
use Webfactory\ContentMapping\ProgressListenerInterface;
use Webfactory\ContentMapping\UpdateableObjectProviderInterface;

/**
 * Adapter for the solarium Solr client as a destination system.
 *
 * @final by default
 */
final class SolariumDestinationAdapter implements DestinationAdapter, ProgressListenerInterface, UpdateableObjectProviderInterface
{
    /**
     * @var Client
     */
    private $solrClient;

    /**
     * @var LoggerInterface
     */
    private $logger;
    
    /**
     * @var int Number of documents to collect before flushing intermediate results to Solr.
     */
    private $batchSize;

    /**
     * @var DocumentInterface[]
     */
    private $newOrUpdatedDocuments = array();

    /**
     * @var string[]|int[]
     */
    private $deletedDocumentIds = array();

    /**
     * @param Client $solrClient
     * @param LoggerInterface $logger
     */
    public function __construct(Client $solrClient, LoggerInterface $logger, $batchSize = 20)
    {
        $this->solrClient = $solrClient;
        $this->logger = $logger;
        $this->batchSize = $batchSize;
    }

    /**
     * @param string $objectClass Fully qualified class name of the object
     * @return \ArrayIterator
     */
    public function getObjectsOrderedById($objectClass)
    {
        $normalizedObjectClass = $this->normalizeObjectClass($objectClass);
        $query = $this->solrClient->createSelect()
                                  ->setQuery('objectclass:' . $normalizedObjectClass)
                                  ->setStart(0)
                                  ->setRows(1000000)
                                  ->setFields(array('id', 'objectid', 'objectclass', 'hash'))
                                  ->addSort('objectid', 'asc');

        /** @var SelectResult $resultset */
        $resultset = $this->solrClient->execute($query);

        $this->logger->info(
            "SolariumDestinationAdapter found {number} objects for objectClass {objectClass}",
            array(
                'number' => $resultset->getNumFound(),
                'objectClass' => $objectClass,
            )
        );

        return $resultset->getIterator();
    }

    /**
     * @param int $id
     * @param string $className
     * @return DocumentInterface
     */
    public function createObject($id, $className)
    {
        $normalizedObjectClass = $this->normalizeObjectClass($className);

        $updateQuery = $this->solrClient->createUpdate();

        $newDocument = $updateQuery->createDocument();
        $newDocument->id = $normalizedObjectClass . ':' . $id;
        $newDocument->objectid = $id;
        $newDocument->objectclass = $normalizedObjectClass;

        return $newDocument;
    }

    public function prepareUpdate($destinationObject)
    {
        return new Document($destinationObject->getFields());
    }

    /**
     * @param SelectedDocumentInterface $destinationObject
     */
    public function delete($destinationObject)
    {
        $this->deletedDocumentIds[] = $destinationObject->id;
    }

    /**
     * This method is a hook e.g. to notice an external change tracker that the $object has been updated.
     *
     * @param DocumentInterface $objectInDestinationSystem
     */
    public function updated($objectInDestinationSystem)
    {
        $this->newOrUpdatedDocuments[] = $objectInDestinationSystem;
    }

    public function afterObjectProcessed()
    {
        if ((count($this->deletedDocumentIds) + count($this->newOrUpdatedDocuments)) >= $this->batchSize) {
            $this->flush();
        }
    }

    /**
     * {@inheritDoc}
     */
    public function commit()
    {
        $this->flush();
    }

    /**
     * @inheritdoc
     *
     * @param DocumentInterface $objectInDestinationSystem
     * @return int
     */
    public function idOf($objectInDestinationSystem)
    {
        return $objectInDestinationSystem->objectid;
    }

    private function flush()
    {
        $this->logger->info(
            "Flushing {numberInsertsUpdates} inserts or updates and {numberDeletes} deletes",
            array(
                'numberInsertsUpdates' => count($this->newOrUpdatedDocuments),
                'numberDeletes' => count($this->deletedDocumentIds),
            )
        );

        if (count($this->deletedDocumentIds) === 0 && count($this->newOrUpdatedDocuments) === 0) {
            return;
        }

        $update = $this->solrClient->createUpdate();

        if ($this->deletedDocumentIds) {
            $update->addDeleteByIds($this->deletedDocumentIds);
        }

        if ($this->newOrUpdatedDocuments) {
            $update->addDocuments($this->newOrUpdatedDocuments);
        }

        $update->addCommit();
        $this->solrClient->execute($update);

        $this->deletedDocumentIds = array();
        $this->newOrUpdatedDocuments = array();

        $this->logger->debug("Flushed");

        /*
         * Manually trigger garbage collection
         * \Solarium\QueryType\Update\Query\Document\Document might hold a reference
         * to a "helper" object \Solarium\Core\Query\Helper, which in turn references
         * back the document. This circle prevents the normal, refcount-based GC from
         * cleaning up the processed Document instances after we release them.
         *
         * To prevent memory exhaustion, we start a GC cycle collection run.
         */
        $update = null;
        gc_collect_cycles();
    }

    /**
     * @param string $objectClass Fully qualified class name of the object
     * @return string
     */
    private function normalizeObjectClass($objectClass)
    {
        if (substr($objectClass, 0, 1) === '\\') {
            $objectClass = substr($objectClass, 1);
        }
        return str_replace('\\', '-', $objectClass);
    }
}
