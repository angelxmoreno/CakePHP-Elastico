<?php

/**
 * Elastico Behavior
 *
 * @package utils
 * @subpackage utils.models.behaviors
 */
class ElasticoBehavior extends ModelBehavior {

    /**
     * Settings to configure the behavior
     *
     * @var array
     */
    protected $_settings = array();

    /**
     * Default settings
     * 
     * @var array
     */
    protected $_defaults = array(
        'enabled' => true,
        'findCallback' => null,
        'index' => null,
        'type' => null,
        'mapping' => null,
    );
    
    /**
     *
     * @var ElasticoSource 
     */
    protected $_elasticoSource;

    /**
     * Setup this behavior with the specified configuration settings.
     *
     * @param Model $Model Model using this behavior
     * @param array $settings Configuration settings for $Model
     * @return void
     */
    public function setup(\Model $Model, $settings = array()) {
        parent::setup($Model, $settings);
        
        if (!isset($this->_settings[$Model->alias])) {
            $this->_settings[$Model->alias] = $this->_defaults;
        }
        
        $this->_settings[$Model->alias] = array_merge($this->_settings[$Model->alias], $settings);
        $this->_settings[$Model->alias]['type'] = $this->_settings[$Model->alias]['type'] ? $this->_settings[$Model->alias]['type'] : $Model->useTable;
        $this->_settings[$Model->alias]['index'] = $this->_settings[$Model->alias]['index'] ? $this->_settings[$Model->alias]['index'] : $this->getElasticoSource()->config['index'];
    }
    
    public function afterSave(\Model $Model, $created, $options = array()) {
        $continue = parent::afterSave($Model, $created, $options);
        if($continue && $this->_settings[$Model->alias]['enabled']){
            $this->indexById($Model, $Model->id, $created);
        }
        return $continue;
    }
    
    public function afterDelete(\Model $Model) {
        parent::afterDelete($model);
        if($this->_settings[$Model->alias]['enabled']){
            die($this->id);
        }
    }
    
    public function indexById(Model $Model, $id, $created = false){
        if($this->_settings[$Model->alias]['findCallback'] && method_exists($Model, $this->_settings[$Model->alias]['findCallback'])){
                $data = call_user_func_array(array($Model, $this->_settings[$Model->alias]['findCallback']), array($id, $created));
            } else {
                $data = current($Model->read(null, $id));
            }
            $this->_indexDocument($Model, $data);
    }
    
    protected function _indexDocument(Model $Model, $data){
        $this->getElasticoSource()->Client->createIndexIfNotExists($this->_settings[$Model->alias]['index']);
        $typeExists = $this->getElasticoSource()->Client->checkTypeExists($this->_settings[$Model->alias]['type']);
        
        if(!$typeExists && $this->_settings[$Model->alias]['mapping']){
            $this->getElasticoSource()->Client->createMapping($this->_settings[$Model->alias]['mapping'], $this->_settings[$Model->alias]['index']);
        } elseif(!$typeExists && !$this->_settings[$Model->alias]['mapping']){
            $this->getElasticoSource()->Client->createMappingFromSchema($this->_settings[$Model->alias]['type'], $Model->schema(),$this->_settings[$Model->alias]['index']);
        }
        $this->getElasticoSource()->Client->indexDocument($data, $this->_settings[$Model->alias]['type'], $this->_settings[$Model->alias]['index']);
        
        
    }
    
    public function getElasticoSource(){
        if(!$this->_elasticoSource){
            $this->_elasticoSource = ConnectionManager::getDataSource('elastico');
        }
        return $this->_elasticoSource;
    }
    
    

}
