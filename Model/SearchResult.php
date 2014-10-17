<?php
App::uses('ElasticoAppModel', 'Elastico.Model');

class SearchResult extends ElasticoAppModel {
    public $useDbConfig = 'elastico';
    public $useTable = '_all';
}