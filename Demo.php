<?php

/**
 * Created by IntelliJ IDEA.
 * User: shihuipeng
 * Date: 2018/7/4
 * Time: 上午10:53
 */
use Hbase\ColumnDescriptor;
use Hbase\Mutation;

require './Hbase.php';

class Demo
{
    public function getRow()
    {
        $hbase = new Hbase();
        $hbase->getRow('user', 'rowkey-2');
    }

    public function get()
    {
        $hbase = new Hbase();
        $hbase->get('user', 'rowkey-2', 'id:2');
    }

    public function createTable()
    {
        $hbase = new Hbase();
        $table = 'user';
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

        $hbase->createTable($table, $columns);
    }

    public function dropTable()
    {
        $table = 'user';
        $hbase = new Hbase();
        $hbase->deleteTable($table);
    }

    public function put()
    {
        $hbase = new Hbase();
        $data = array(
            new mutation(
                array('column' => 'id:2', 'value' => '2')
                //array('column' => 'name:1', 'value' => '史慧鹏'),
                //array('column' => 'score:1', 'value' => '100')
            )
        );

        $hbase->put('user', 'rowkey1', $data);
    }

    public function deleteAllRow()
    {
        $hbase = new Hbase();
        $hbase->deleteAllRow('user', 'rowkey-2');
    }

    public function deleteAll()
    {
        $hbase = new Hbase();
        $hbase->deleteAll('user', 'rowkey-1', 'score:1');
    }

    public function scan()
    {
        $hbase = new Hbase();
        $hbase->scan('user');
    }


}

$demo = new Demo();

/**
 * 创建 table
 */

//$demo->createTable();
/**
 * 删除 table
 */
//$demo->dropTable();

/**
 * 添加行 列
 */
//$demo->put();

/**
 * 获取 行
 */
//$demo->getRow();

/**
 * 获取行->列
 */
//$demo->get();

/**
 * 删除行
 */
//$demo->deleteAllRow();

/**
 * 删除行-列
 */
//$demo->deleteAll();

/**
 * scan
 */
$demo->scan();