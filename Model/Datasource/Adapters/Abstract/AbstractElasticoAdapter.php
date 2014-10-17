<?php

abstract class AbstractElasticoAdapter extends object {
    
    protected $_cakeToESCols = array(
        'biginteger' => 'long',
        'text' => 'string',
        'datetime' => 'date'
    );
    abstract public function connect(array $configs);

    abstract public function listIndices();

    abstract public function fetchDocument($id, $type, $index = null);

    abstract public function simpleSearch($query, $type, $index = null);
    
    abstract public function createMappingFromSchema($type, array $schema, $index = null);
    
    abstract public function createMapping($params, $index = null);
    
    abstract public function checkIndexExists($index = null);
    
    abstract public function checkTypeExists($type, $index = null);
    
    abstract public function createIndex($index = null);
    
    abstract public function createType($type, $index = null);
    /**
     * Abstract function to get list of document names for the default index
     * 
     * @return array An array of available document names like so:
     *  array(
     *      (int) 0 => 'users',
     *      i(nt) 1 => 'pets'
     *  )
     */
    abstract public function listSources($data = null);

    abstract public function describe($type, $index = null);

    abstract public function read(Model $Model, array $queryData = array(), $recursive = null);

    abstract public function create(Model $Model, $fields = null, $values = null);
    
    abstract public function update(Model $Model, $fields = null, $values = null, $conditions = null);
    
    abstract public function delete(Model $Model, $conditions = null);
    
    public function cake2ESCol($cake_col){
        if(array_key_exists($cake_col, $this->_cakeToESCols)){
            return $this->_cakeToESCols[$cake_col];
        }
        return $cake_col;
    }
    
    public function cake2ESSchema(array $schema){
        if(!$schema){
            return array();
        }
        
        $mapping = array();
        foreach($schema as $field_name => $field_properties){
            $mapping['properties'][$field_name] = array(
                'type' => $this->cake2ESCol($field_properties['type'])
            );
            if($field_properties['type'] == 'datetime'){
                $mapping['properties'][$field_name]['format'] = 'yyyy-MM-dd HH:mm:ss';
            }
        }
        return $mapping;
    }
    
    public function createIndexIfNotExists($index = null){
        $index = $this->_getIndex($index);
        if(!$this->checkIndexExists($index)){
            $this->createIndex($index);
        }
    }
    
    public function createTypeIfNotExists($type, $index = null){
        $index = $this->_getIndex($index);
        if(!$this->checkTypeExists($type)){
            $this->createType($type);
        }
    }
    
    protected function _getIndex($index = null){
        $index = !$index ? $this->index : $index;
        return $index;
    }
}
