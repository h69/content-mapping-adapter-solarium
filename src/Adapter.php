<?php
namespace H69\ContentMapping\Solarium;

use Solarium\Client;
use Solarium\QueryType\Select\Result\Result;
use Solarium\QueryType\Update\Query\Document\DocumentInterface;
use Solarium\QueryType\Update\Query\Document\Document;
use H69\ContentMapping as CM;

/**
 * Class Adapter
 * Adapter for the solarium Solr client
 *
 * @package H69\ContentMapping\Solarium
 */
class Adapter implements CM\Adapter, CM\Adapter\ProgressListener, CM\Adapter\UpdateableObjectProvider
{
    /**
     * @var Client
     */
    private $solrClient;

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
     * @var array
     */
    protected $messages = [];

    /**
     * @param Client $solrClient
     */
    public function __construct($solrClient, $batchSize = 20)
    {
        if (!$solrClient instanceof Client) {
            throw new \InvalidArgumentException('solr client have to be an instance of Solarium\Client');
        }

        $this->solrClient = $solrClient;
        $this->batchSize = $batchSize;
    }

    /**
     * @return array
     */
    public function getMessages()
    {
        return $this->messages;
    }

    /**
     * Get an Iterator over all $type objects in the source/destination system, ordered by their ascending IDs.
     *
     * @param string $type       Type of Objects to return
     * @param string $indexQueue Whether all Objects or only new, updated or deleted Objects are returned for indexing
     *
     * @return \Iterator
     */
    public function getObjectsOrderedById($type, $indexQueue = false)
    {
        if ($indexQueue !== false) {
            throw new NotImplementedException(__CLASS__ . ' is not available for indexing! (Status of objects can\'t be determined)');
        }

        $query = $this->solrClient->createSelect()
            ->setQuery('type:' . $type)
            ->setStart(0)
            ->setRows(1000000)
            ->setFields(array('*'))
            ->addSort('id', 'asc');

        /** @var Result $resultset */
        $resultset = $this->solrClient->execute($query);

        $this->messages[] = "SolariumAdapter found " . $resultset->getNumFound() . " objects for type " . $type;
        return $resultset->getIterator();
    }

    /**
     * Get the id of an object
     *
     * @param mixed $object
     *
     * @return int
     */
    public function idOf($object)
    {
        return $object->id;
    }

    /**
     * Get the current status of an object (NEW, UPDATE or DELETE)
     *
     * @param mixed $object
     *
     * @return int
     */
    public function statusOf($object)
    {
        throw new NotImplementedException(__CLASS__ . ' is not available for indexing! (Status of objects can\'t be determined)');
    }

    /**
     * Create a new object in the target system identified by ($id and $type).
     *
     * @param int    $id   ID of the newly created Object
     * @param string $type Type of the newly created Object
     *
     * @return mixed
     */
    public function createObject($id, $type)
    {
        $newDocument = $this->solrClient->createUpdate()->createDocument();
        $newDocument->id = $id;
        $newDocument->type = $type;
        return $newDocument;
    }

    /**
     * Delete the $object from the target system.
     *
     * @param mixed $object
     */
    public function delete($object)
    {
        $this->deletedDocumentIds[] = $object->id;
    }

    /**
     * This method is a hook e.g. to notice an external change tracker that the $object has been updated.
     *
     * Although the name is somewhat misleading, it will be called after the Mapper has processed
     *   a) new objects created by the createObject() method
     *   b) changed objects created by the prepareUpdate() method *only if* the object actually changed.
     *
     * @param mixed $object
     */
    public function updated($objectInDestinationSystem)
    {
        $this->newOrUpdatedDocuments[] = $objectInDestinationSystem;
    }

    /**
     * This method is a hook e.g. to notice an external change tracker that all the in memory synchronization is
     * finished, i.e. can be persisted (e.g. by calling an entity manager's flush()).
     */
    public function commit()
    {
        if (count($this->deletedDocumentIds) === 0 && count($this->newOrUpdatedDocuments) === 0) {
            return;
        }
        
        $this->messages[] = "Flushing " . count($this->newOrUpdatedDocuments) . " inserts or updates and " . count($this->deletedDocumentIds) . " deletes";

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
     * Create the object instance that can be used to update data in the target system.
     *
     * @param mixed $object A destination object as returned from getObjectsOrderedById()
     *
     * @return mixed The (possibly new) object that will be passed to the mapping function.
     */
    public function prepareUpdate($object)
    {
        return new Document($object->getFields());
    }

    /**
     * Callback method that will be called after every single object has been processed.
     *
     * @return void
     */
    public function afterObjectProcessed()
    {
        if ((count($this->deletedDocumentIds) + count($this->newOrUpdatedDocuments)) >= $this->batchSize) {
            $this->commit();
        }
    }
}
