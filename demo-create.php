<?php

$GLOBALS['THRIFT_ROOT'] = './thrift/lib/Thrift';

require_once('thrift/src/Thrift.php');

require_once($GLOBALS['THRIFT_ROOT'] . '/Type/TMessageType.php');
require_once($GLOBALS['THRIFT_ROOT'] . '/Type/TType.php');
require_once($GLOBALS['THRIFT_ROOT'] . '/Exception/TException.php');
require_once($GLOBALS['THRIFT_ROOT'] . '/Factory/TStringFuncFactory.php');
require_once($GLOBALS['THRIFT_ROOT'] . '/StringFunc/TStringFunc.php');
require_once($GLOBALS['THRIFT_ROOT'] . '/StringFunc/Core.php');
require_once($GLOBALS['THRIFT_ROOT'] . '/Transport/TSocket.php');
require_once($GLOBALS['THRIFT_ROOT'] . '/Transport/TBufferedTransport.php');
require_once($GLOBALS['THRIFT_ROOT'] . '/Protocol/TBinaryProtocol.php');

require_once('thrift/lib/HBase/Hbase.php');
require_once('thrift/lib/HBase/Types.php');

use Hbase\ColumnDescriptor;
use Thrift\Transport\TSocket;
use Thrift\Transport\TBufferedTransport;
use Thrift\Protocol\TBinaryProtocol;
use Hbase\HbaseClient;
use Hbase\Mutation;

try {
    //$socket = new TSocket('127.0.0.1', 9090);
    $socket = new TSocket('10.1.1.187', 9090);
    $socket->setSendTimeout(10000); // Ten seconds (too long for production, but this is just a demo ;)
    $socket->setRecvTimeout(20000); // Twenty seconds
    $transport = new TBufferedTransport($socket);
    $protocol = new TBinaryProtocol($transport);
    $client = new HbaseClient($protocol);


    $transport->open();

    $table = 'hello1';
    $rowkey = 'row1';
    $column = 'cf:a';


    $columns = array(
        new ColumnDescriptor(array(
            'name' => 'id:',
            'maxVersions' => 10
        )),
        new ColumnDescriptor(array(
            'name' => 'name:'
        )),
        new ColumnDescriptor(array(
            'name' => 'score:'
        )));


    $client->createTable($table, $columns);

    //$time = time();
    //$data = array(
    //    new mutation(
    //        array('column' => 'id:' . $time, 'value' => $time),
    //        array('column' => 'name:' . $time, 'value' =>"嗨少年"),
    //        array('column' => 'score:' . $time, 'value' =>$time)
    //    )
    //);
    //
    //
    //$client->mutateRow('hello', 'row1', $data, null);

    $transport->close();

} catch (TException $e) {
    print 'TException: ' . $e->__toString() . ' Error: ' . $e->getMessage() . "\n";
}
