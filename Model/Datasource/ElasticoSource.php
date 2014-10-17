<?php

/**
 * Elastic Search Datasource
 *
 * A CakePHP datasource for interacting with Elasticsearch.
 *
 * Create a datasource in your config/database.php
 *  public $elastico = array(
 *    'datasource' => 'Elastico.ElasticoSource',
 *    'key' => 'PUBLIC KEY',
 *    'secret' => 'SECRET KEY',
 *    'tag' => 'YOUR ASSOCIATE ID',
 *    'locale' => 'com' //(ca,com,co,uk,de,fr,jp)
 *  );
 */
App::uses('DataSource', 'Model/Datasource');
App::uses('AbstractElasticoAdapter', 'Elastico.Model/Datasource/Adapters/Abstract');

/**
 * Elasticsearch Datasource
 *
 */
class ElasticoSource extends DataSource {

    /**
     * Description of datasource
     *
     * @var string
     */
    public $description = 'Elasticsearch Datasource';

    /**
     * Whether or not source data like available tables and schema descriptions
     * should be cached
     *
     * @var bool
     */
    public $cacheSources = true;

    /**
     * Holds a list of sources (tables) contained in the DataSource
     *
     * @var array
     */
    protected $_sources = null;

    /**
     * The default configuration of a specific DataSource
     *
     * @var array
     */
    protected $_baseConfig = array(
        //'index' => 'test',
        'servers' => array('http://localhost:9200/'),
        'adapterClass' => 'ElasticsearchPHPAdapter',
    );

    /**
     * Query array
     *
     * @var array
     */
    public $query = null;

    /**
     * Signed request string to pass to Amazon
     *
     * @var string
     */
    protected $_request = null;

    /**
     * Client object
     *
     * @var AbstractElasticoAdapter
     */
    public $Client = null;
    public $columns = array(
        'date' => array('name' => 'date', 'format' => 'Y-m-d H:i:s', 'formatter' => 'date'),
    );

    /**
     * Request Logs
     *
     * @var array
     */
    protected $__requestLog = array();

    /**
     * Constructor
     *
     * Creates new HttpSocket
     *
     * @param array $config Configuration array
     */
    public function __construct($config) {
        parent::__construct($config);
        App::uses($this->config['adapterClass'], 'Elastico.Model/Datasource/Adapters');
        // Check that the class exists before trying to use it
        if (!class_exists($this->config['adapterClass'])) {
            throw new CakeException('The Elastico Adapter class: "' . $this->config['adapterClass'] . '" could not be found.');
        }

        $class = $this->config['adapterClass'];
        $client = new $class();

        if (!($client instanceof AbstractElasticoAdapter)) {
            throw new CakeException('The adapter "' . $this->config['adapterClass'] . '" is not of type: "AbstractElasticoAdapter"');
        }

        $this->Client = $client->connect($this->config);
    }

    /**
     * Caches/returns cached results for child instances
     *
     * @param mixed $data Unused in this class.
     * @return array Array of sources available in this datasource.
     */
    public function listSources($data = null) {
        return $this->Client->listSources($data);
    }

    /**
     * Returns a Model description (metadata) or null if none found.
     *
     * @param Model|string $model The model to describe.
     * @return array Array of Metadata for the $model
     */
    public function describe($model) {
        if (is_string($model)) {
            $table = $model;
        } else {
            $table = $model->tablePrefix . $model->table;
        }
        return $this->Client->describe($table);
    }

    /**
     * Returns an calculation
     *
     * @param model $model
     * @param string $type Lowercase name type, i.e. 'count' or 'max'
     * @param array $params Function parameters (any values must be quoted manually)
     * @return string Calculation method
     */
    public function calculate(Model $model, $type, $params = array()) {
        return 'COUNT';
    }

    /**
     * Used to read records from the source. The "R" in CRUD
     *
     * @param Model $Model The model being read.
     * @param array $queryData An array of query data used to find the data you want
     * @param int $recursive Number of levels of association
     * @return mixed
     */
    public function read(Model $Model, $queryData = array(), $recursive = null) {
        $queryData['conditions'] = (array) $queryData['conditions'];
        if ($queryData['conditions']) {
            foreach ($queryData['conditions'] as $key => $val) {
                if (strpos($key, '.')) {
                    list($alias, $field) = explode('.', $key, 2);
                    $queryData['conditions'][$field] = $val;
                    unset($queryData['conditions'][$key]);
                }
            }
        }
        /*
         * @todo
         *  1. Add Pagination
         *  2. add limit
         *  3. add whitelisting of fields
         *  4. add more query types
         *  5. allow complex queries as $queryData['query']
         *  6. allow complex filtering as $queryData['filter']
         */
        $results = $this->Client->read($Model, $queryData, $recursive);
        if ($queryData['fields'] === 'COUNT') {
            return array(array(array('count' => count($results))));
        }

        if ($results) {
            foreach ($results as $result) {
                $_results[] = array($Model->alias => $result);
            }
            return $_results;
        }
        return array();
    }

    /**
     * Used to create new records. The "C" CRUD.
     *
     *
     * @param Model $Model The Model to be created.
     * @param array $fields An Array of fields to be saved.
     * @param array $values An Array of values to save.
     * @return bool success
     */
    public function create(Model $Model, $fields = null, $values = null) {
        return $this->Client->create($Model, $fields, $values);
    }

    /**
     * Update a record(s) in the datasource.
     *
     * @param Model $Model Instance of the model class being updated
     * @param array $fields Array of fields to be updated
     * @param array $values Array of values to be update $fields to.
     * @param mixed $conditions The array of conditions to use.
     * @return bool Success
     */
    public function update(Model $Model, $fields = null, $values = null, $conditions = null) {
        return $this->Client->update($Model, $fields, $values, $conditions);
    }

    /**
     * Delete a record(s) in the datasource.
     *
     * To-be-overridden in subclasses.
     *
     * @param Model $Model The model class having record(s) deleted
     * @param mixed $conditions The conditions to use for deleting.
     * @return bool Success
     */
    public function delete(Model $Model, $conditions = null) {
        $this->Client->delete($Model, $conditions);
    }

    /**
     * Play nice with the DebugKit
     *
     * @param boolean sorted ignored
     * @param boolean clear will clear the log if set to true (default)
     * @return array of log requested
     */
    public function getLog($sorted = false, $clear = true) {
        $log = $this->__requestLog;
        if ($clear) {
            $this->__requestLog = array();
        }
        return array('log' => $log, 'count' => count($log), 'time' => 'Unknown');
    }

    public function query($method, $arguments, Model $Model) {
        if (method_exists($this, $method)) {
            return call_user_func_array(array($this, $method), array('Model' => $Model) + $arguments);
        }

        if (method_exists($this->Client, $method)) {
            return call_user_func_array(array($this->Client, $method), $arguments);
        }
        throw new CakeException(__d('cake_dev', 'Method %1$s::%2$s does not exist', get_class($this), $method));
    }

}
