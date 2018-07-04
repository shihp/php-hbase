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

use Thrift\Transport\TSocket;
use Thrift\Transport\TBufferedTransport;
use Thrift\Protocol\TBinaryProtocol;
use Hbase\HbaseClient;
use Hbase\Mutation;
use Hbase\TScan;

try {
    $socket = new TSocket('127.0.0.1', 9090);
    $socket->setSendTimeout(10000); // Ten seconds (too long for production, but this is just a demo ;)
    $socket->setRecvTimeout(20000); // Twenty seconds
    $transport = new TBufferedTransport($socket);
    $protocol = new TBinaryProtocol($transport);
    $client = new HbaseClient($protocol);


    $transport->open();

    $table = 'hello';
    $rowkey = 'row1';
    $column = 'name:1530671942';

    //print_r($client->getRow($table, $rowkey, null));
    print_r($client->get($table, $rowkey, $column, null));

    $transport->close();

} catch (TException $e) {
    print 'TException: ' . $e->__toString() . ' Error: ' . $e->getMessage() . "\n";
}
