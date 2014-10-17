<?php

App::uses('Shell', 'Console/Command');

abstract class AbstractCliTask extends Shell {

    public $uses = array();

    public function execute() {
        
    }

}
