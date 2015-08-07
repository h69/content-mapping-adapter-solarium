<?php
/*
 * (c) webfactory GmbH <info@webfactory.de>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Webfactory\ContentMapping\Solr;

use Psr\Log\LoggerInterface;
use Solarium\Client;
use Solarium\QueryType\Select\Result\DocumentInterface as SelectedDocumentInterface;;
use Solarium\QueryType\Select\Result\Result as SelectResult;
use Solarium\QueryType\Update\Query\Document\DocumentInterface as UpdatedDocumentInterface;
use Webfactory\ContentMapping\DestinationAdapter;

/**
 * Adapter for the solarium Solr client as a destination system.
 *
 * @final by default
 */
final class SolariumDestinationAdapter implements DestinationAdapter
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
     * @var UpdatedDocumentInterface[]
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
    public function __construct(Client $solrClient, LoggerInterface $logger)
    {
        $this->solrClient = $solrClient;
        $this->logger = $logger;
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
     * @return UpdatedDocumentInterface
     */
    public function createObject($id, $className)
    {
        $normalizedObjectClass = $this->normalizeObjectClass($className);

        $updateQuery = $this->solrClient->createUpdate();

        $newDocument = $updateQuery->createDocument();
        $newDocument->id = $normalizedObjectClass . ':' . $id;
        $newDocument->objectid = $id;
        $newDocument->objectclass = $normalizedObjectClass;

        $updateQuery->addDocument($newDocument)
                    ->addCommit();

        $this->solrClient->execute($updateQuery);

        return $newDocument;
    }

    /**
     * @param SelectedDocumentInterface $destinationObject
     */
    public function delete($destinationObject)
    {
        $this->deletedDocumentIds[] = $destinationObject->id;
        $this->flushIfBatchIsBigEnough();
    }

    /**
     * This method is a hook e.g. to notice an external change tracker that the $object has been updated.
     *
     * @param UpdatedDocumentInterface $objectInDestinationSystem
     */
    public function updated($objectInDestinationSystem)
    {
        $this->newOrUpdatedDocuments[] = $objectInDestinationSystem;
        $this->flushIfBatchIsBigEnough();
    }

    /**
     * {@inheritDoc}
     */
    public function commit()
    {
        $this->flush();
    }

    /**
     * Get the id of an \Apache_Solr_Document object in the destination system.
     *
     * @param UpdatedDocumentInterface $objectInDestinationSystem
     * @return int
     */
    public function idOf($objectInDestinationSystem)
    {
        return $objectInDestinationSystem->objectid;
    }

    private function flushIfBatchIsBigEnough()
    {
        if ((count($this->deletedDocumentIds) + count($this->newOrUpdatedDocuments)) >= 20) {
            $this->flush();
        }
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

        if ($this->deletedDocumentIds) {
            $updateQuery = $this->solrClient->createUpdate()
                                            ->addDeleteByIds($this->deletedDocumentIds)
                                            ->addCommit();
            $this->solrClient->execute($updateQuery);
            $this->deletedDocumentIds = array();
        }

        if ($this->newOrUpdatedDocuments) {
            $updateQuery = $this->solrClient->createUpdate()
                                            ->addDocuments($this->newOrUpdatedDocuments)
                                            ->addCommit();
            $this->solrClient->execute($updateQuery);
            $this->newOrUpdatedDocuments = array();
        }

        $this->logger->debug("Flushed");
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
