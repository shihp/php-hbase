<?php

/**
 * Created by IntelliJ IDEA.
 * User: shihuipeng
 * Date: 2018/7/4
 * Time: 上午10:41
 */


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
use Hbase\HbaseClient;
use Hbase\Mutation;
use Hbase\TScan;
use Thrift\Protocol\TBinaryProtocol;
use Thrift\Transport\TBufferedTransport;
use Thrift\Transport\TSocket;


class Hbase
{
    private $client = '';
    private $transport = '';

    public function __construct()
    {
        $socket = new TSocket('localhost', 9090);
        //$socket = new TSocket('10.1.1.187', 9090);
        $socket->setSendTimeout(10000); // Ten seconds (too long for production, but this is just a demo ;)
        $socket->setRecvTimeout(20000); // Twenty seconds
        $this->transport = new TBufferedTransport($socket);
        $protocol = new TBinaryProtocol($this->transport);
        $this->client = new HbaseClient($protocol);
    }

    public function getRow($table, $rowkey)
    {
        try {
            $this->transport->open();
            print_r($this->client->getRow($table, $rowkey, null));
            $this->transport->close();

        } catch (TException $e) {
            print 'TException: ' . $e->__toString() . ' Error: ' . $e->getMessage() . "\n";
        }
    }

    public function get($table, $rowkey, $column)
    {
        try {
            $this->transport->open();
            print_r($this->client->get($table, $rowkey, $column, null));
            $this->transport->close();
        } catch (TException $e) {
            print 'TException: ' . $e->__toString() . ' Error: ' . $e->getMessage() . "\n";
        }
    }

    public function put($table, $rowkey, $data)
    {
        try {

            $this->transport->open();

            $this->client->mutaterow($table, $rowkey, $data, null);

            $this->transport->close();

        } catch (TException $e) {
            exit('TException: ' . $e->__toString() . ' Error: ' . $e->getMessage() . "\n");
        }
    }

    public function deleteAllRow($table, $row, $attributes = null)
    {
        try {

            $this->transport->open();

            $this->client->deleteAllRow($table, $row, $attributes);

            $this->transport->close();

        } catch (TException $e) {
            exit('TException: ' . $e->__toString() . ' Error: ' . $e->getMessage() . "\n");
        }
    }

    public function deleteAll($table, $row, $column, $attributes = null)
    {
        try {

            $this->transport->open();

            $this->client->deleteAll($table, $row, $column, $attributes);

            $this->transport->close();

        } catch (TException $e) {
            exit('TException: ' . $e->__toString() . ' Error: ' . $e->getMessage() . "\n");
        }
    }

    public function scan($table, $filter = null, $sortColumns = true)
    {
        try {

            $this->transport->open();
            $scan = new TScan(
                array(
                    //'startRow' => 'rowkey4',
                    //'stopRow' => 'rowkey2',
                    'filterString' => $filter,
                    'sortColumns' => $sortColumns
                )
            );
            $scanid = $this->client->scannerOpenWithScan($table, $scan, null);
            $rowresult = $this->client->scannerGet($scanid);
            $this->transport->close();
            var_dump($rowresult);

        } catch (TException $e) {
            exit('TException: ' . $e->__toString() . ' Error: ' . $e->getMessage() . "\n");
        }
    }

    public function createTable($table, $columns)
    {
        try {
            $this->transport->open();
            $this->client->createTable($table, $columns);
            $this->transport->close();

        } catch (TException $e) {
            print 'TException: ' . $e->__toString() . ' Error: ' . $e->getMessage() . "\n";
        }
    }

    public function deleteTable($table)
    {
        try {
            $this->transport->open();
            $this->client->disableTable($table);
            $this->client->deleteTable($table);
            $this->transport->close();
        } catch (TException $e) {
            print 'TException: ' . $e->__toString() . ' Error: ' . $e->getMessage() . "\n";
        }
    }
}