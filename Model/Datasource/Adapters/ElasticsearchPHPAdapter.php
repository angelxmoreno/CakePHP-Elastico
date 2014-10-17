<?php

App::uses('AbstractElasticoAdapter', 'Elastico.Model/Datasource/Adapters/Abstract');

class ElasticsearchPHPAdapter extends AbstractElasticoAdapter {

    /**
     *
     * @var Elasticsearch\Client 
     */
    public $Client;
    public $index;
    protected $_config = array();
    protected $_config_keys = array(
        'hosts',
        'connectionParams'
    );

    public function connect(array $configs) {
        $config['hosts'] = isset($configs['servers']) ? $configs['servers'] : array('http://localhost:9200');
        $config['connectionParams'] = isset($configs['connectionParams']) ? $configs['connectionParams'] : array();
        $this->index = isset($configs['index']) ? $configs['index'] : 'test';
        $this->Client = new Elasticsearch\Client($config);
        return $this;
    }

    public function listIndices() {
        $response = $this->Client->indices()->status(array('index'));
        return array_keys($response['indices']);
    }

    public function createMappingFromSchema($type, array $schema, $index = null) {
        $index = $this->_getIndex($index);
        $mapping = $this->cake2ESSchema($schema);
        if ($mapping) {
            $params['index'] = $index;
            $params['type'] = $type;
            $params['body'][$type] = $mapping;
        }
        $this->createMapping($params, $index);
    }

    public function createMapping($params, $index = null) {
        $index = $this->_getIndex($index);
        $params = am(array('index' => $index), $params);
        $this->Client->indices()->putMapping($params);
    }

    public function checkIndexExists($index = null) {
        $index = $this->_getIndex($index);
        //debug($index);
        //debug($this->Client->indices()->exists(array('index' => 'lol')));die;
        return $this->Client->indices()->exists(array('index' => $index));
    }

    public function checkTypeExists($type, $index = null) {
        $index = $this->_getIndex($index);
        return $this->Client->indices()->existsType(array('index' => $index, 'type' => $type));
    }

    public function createIndex($index = null) {
        $index = $this->_getIndex($index);
        if (!$this->checkIndexExists($index)) {
            $created = $this->Client->indices()->create(array('index' => $index));
            if (!isset($created['acknowledged'])) {
                return false;
            }
            return $created['acknowledged'];
        }
        return true;
    }

    public function createType($type, $index = null) {
        //@todo create type with an id
    }

    public function fetchDocument($id, $type, $index = null) {
        $index = $this->_getIndex($index);
        $params = array(
            'id' => $id,
            'type' => $type,
            'index' => $index,
        );
        /*
         * Unbelievable but true :https://github.com/elasticsearch/elasticsearch-php/issues/37
         */
        try {
            $response = $this->Client->get($params);
        } catch (Exception $e) {
            if ($e->getCode() == '404' && json_decode($e->getMessage(), true)) {
                $response = json_decode($e->getMessage(), true);
            } elseif ($e->getCode() == '400') {
                return array();
            } else {
                throw $e;
            }
        }

        if ($response['found']) {
            $result = array('id' => $response['_id']) + $response['_source'];
            return array($result);
        }
        return array();
    }

    public function simpleSearch($conditions, $type, $index = null) {
        $index = $this->_getIndex($index);
        $conditions = is_string($conditions) ? array('_all' => $conditions) : $conditions;

        $query = array(
            'match' => $conditions
        );
        return $this->_search($type, $index, $query);
    }

    public function fetchAll($type, $index = null) {
        $index = $this->_getIndex($index);
        return $this->_search($type, $index);
    }

    protected function _search($type, $index, $query = null) {
        $query = is_string($query) ? array('_all' => $query) : $query;

        $params['index'] = $index;
        $params['type'] = $type;
        if ($query) {
            $params['body']['query'] = $query;
        }

        $response = $this->Client->search($params);
        $results = array();
        foreach ($response['hits']['hits'] as $result) {
            $results[] = array('id' => $result['_id']) + $result['_source'];
        }
        return $results;
    }

    public function indexDocument($data, $type, $index = null) {
        $index = $this->_getIndex($index);
        $id = (isset($data['id'])) ? $data['id'] : String::uuid();

        $params['index'] = $index;
        $params['type'] = $type;
        $params['id'] = $id;
        $params['body'] = $data;

        try {
            $response = $this->Client->index($params);
        } catch (ElasticSearch\Exception $e) {
            return false;
        }
        return true;
    }

    public function listSources($data = null, $index = null) {
        $index = $this->_getIndex($index);
        $response = $this->Client->indices()->getMapping(array('index' => $index));
        debug(array('_all') + array_keys($response[$this->index]['mappings']));die;
        return array('_all') + array_keys($response[$this->index]['mappings']);
    }

    public function describe($type, $index = null) {
        $index = $this->_getIndex($index);
        $params = array(
            'type' => $type,
            'index' => $index,
        );
        $response = $this->Client->indices()->getMapping($params);
        if ($response) {
            $schema = $response[$this->index]['mappings'][$type]['properties'];
            //lets always have id in the schema
            if (!array_key_exists('id', $schema)) {
                $schema['id'] = array('type' => 'string');
            }
            return $schema;
        }

        return array();
    }

    public function read(Model $Model, array $queryData = array(), $recursive = null) {
        //check if id is passed
        if (array_key_exists('id', $queryData['conditions'])) {
            $id = $queryData['conditions']['id'];
            /* this means we are going to fetch the document directly instead of
             * performing a search
             */
            return $this->fetchDocument($id, $Model->useTable);
        }

        if (!$queryData['conditions']) {
            return $this->fetchAll($Model->useTable);
        }

        //at this point assume we are executing a search
        return $this->simpleSearch($queryData['conditions'], $Model->useTable);
    }

    public function create(Model $Model, $fields = null, $values = null) {
        $data = array_combine($fields, $values);
        return $this->indexDocument($data, $Model->useTable);
    }

    public function update(Model $Model, $fields = null, $values = null, $conditions = null) {
        $data = array_combine($fields, $values);
        if (!$conditions) {
            return $this->indexDocument($data, $Model->useTable);
        } else {
            throw new CakeException('Update with conditions is not yet implemented');
        }
    }

    public function delete(Model $Model, $conditions = null) {
        debug($conditions);
        die;
    }

}
