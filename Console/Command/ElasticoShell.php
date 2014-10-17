<?php

App::uses('AppShell', 'Console/Command');

/**
 * Elastico Shell file
 *
 */
class ElasticoShell extends AppShell {

    public $tasks = array('IndexCli', 'TypeCli');

    public function indexModel() {
        if (!isset($this->args[0])) {
            $this->outError('No Model supplied.');
            return false;
        }

        $model2find = $this->args[0];

        if (!$this->_validateModel($model2find)) {
            $this->outError('No Model supplied.');
            return false;
        }
        //load the model
        $ModelInstance = $this->_loadModel($model2find);

        //check if the Model has the Elastico behavior
        if ($has_elastico_behavior = $this->_hasElasticoBehavior($ModelInstance)) {
            $this->out('Found Elastico Behavior.');
        } else {
            $this->out('No Elastico Behavior found.');
        }

        $continue = strtolower($this->in('Continue...?', array('y', 'n'), 'y'));
        if ($continue == 'y') {
            if($has_elastico_behavior){
                return $this->_indexElasticoModel($ModelInstance);
            } else {
                return $this->_indexNonElasticoModel($ModelInstance);
            }
        }
        $this->err('User aborted');
        return false;
    }

    protected function _validateModel($model2find) {
        list($plugin, $model) = pluginSplit($model2find, false);

        if ($plugin) {
            $known_plugins = App::objects('plugin');
            if (!in_array($plugin, $known_plugins)) {
                $this->out('<error>The plugin given "' . $plugin . '" does not to be valid.</error>');
                return false;
            }
            $plugin .= '.';
        }

        $known_models = App::objects($plugin . 'model');
        if (!in_array($model, $known_models)) {
            $this->out('<error>The Model given "' . $model2find . '" does not to be valid.</error>');
            return false;
        }

        return true;
    }

    protected function _loadModel($model2find) {
        return ClassRegistry::init($model2find);
    }

    protected function _hasElasticoBehavior(Model $Model) {
        return isset($Model->actsAs['Elastico.Elastico']);
    }

    protected function _indexElasticoModel(Model $Model) {
        $num_rows = $Model->find('count');
        $limit = 50;
        $max_pages = ceil($num_rows / $limit);
        $this->out('About to index the model ' . $Model->alias);
        $this->out('Cycling through ' . $num_rows . ' in ', 0);
        for ($count_down = 5; $count_down >= 0; $count_down--) {
            $this->out($count_down, 0);
            sleep(1);
            if ($count_down > 0) {
                $this->out('...', 0);
            }
        }
        $current_row = 0;
        for ($current_page = 1; $current_page <= $max_pages; $current_page++) {
            $params = array(
                'limit' => $limit,
                'page' => $current_page,
                'order' => $Model->alias . '.' . $Model->primaryKey,
                    //'recursive' => 1,
            );
            $rows = $Model->find('list', $params);
            foreach ($rows as $id => $name) {
                $current_row++;
                $this->out('Row ' . $current_row . ' of ' . $num_rows . ' -- Indexing the ' . $Model->alias . ' with id#' . $id . ' -- '.$name);
                $Model->indexById($id);
            }
        }
    }
    
    protected function _indexNonElasticoModel(Model $Model){
        //load the Elastico DS
        $db = ConnectionManager::getDataSource('elastico');

        //create the type on elastic search
        $type = $Model->useTable.'_results';
        $db->Client->createIndexIfNotExists();
        if($db->Client->checkTypeExists($type)){
            $db->createMappingFromSchema($Model->useTable, $Model->schema());
        }
        $num_rows = $Model->find('count');
        $limit = 50;
        $max_pages = ceil($num_rows / $limit);
        $this->out('About to index the model ' . $Model->alias);
        $this->out('Cycling through ' . $num_rows . ' in ', 0);
        for ($count_down = 5; $count_down >= 0; $count_down--) {
            $this->out($count_down, 0);
            sleep(1);
            if ($count_down > 0) {
                $this->out('...', 0);
            }
        }
        $current_row = 0;
        for ($current_page = 1; $current_page <= $max_pages; $current_page++) {
            $params = array(
                'limit' => $limit,
                'page' => $current_page,
                'order' => $Model->alias . '.' . $Model->primaryKey,
                'recursive' => -1,
            );
            $rows = $Model->find('all', $params);
            foreach ($rows as $row) {
                $current_row++;
                $this->out('Row ' . $current_row . ' of ' . $num_rows . ' -- Indexing the ' . $Model->alias . ' with id#' . $row[$Model->alias][$Model->primaryKey]);
                $doc = $row[$Model->alias];
                unset($row[$Model->alias]);
                $doc = am($doc, $row);
                $db->Client->indexDocument($doc, $type);
            }
        }
    }

    public function outError($message = null, $newlines = 1, $level = Shell::NORMAL) {
        $message = '<error>' . $message . '</error>';
        parent::out($message, $newlines, $level);
    }

}
