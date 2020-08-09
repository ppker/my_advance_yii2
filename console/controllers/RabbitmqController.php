<?php
/**
 * Created by PhpStorm
 * User: yanzhipeng
 * Date: 2020/8/4
 * Time: 11:39 上午
 * Desc:
 */


namespace console\controllers;

use Yii;
use yii\console\Controller;
use yii\helpers\Console;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

class RabbitmqController extends Controller {

    protected $_mq_dns = [
        'host' => 'localhost',
        'port' => 5672,
        'user' => 'admin',
        'password' => '123456'
    ];

    public function options($actionID) {

        return array_merge(parent::options($actionID), [
        ]);
    }

    public function optionAliases() {

        return array_merge(parent::optionAliases(), [
        ]);
    }

    public function actionSend() {

        $connection = new AMQPStreamConnection($this->_mq_dns['host'], $this->_mq_dns['port'], $this->_mq_dns['user'], $this->_mq_dns['password']);
        $channel = $connection->channel();
        $channel->queue_declare('hello', false, false, false, false);
        $msg = new AMQPMessage('hello world, i am ppker.');
        $msg1 = new AMQPMessage('zhu zhu zhu, come on for me.');
        $channel->basic_publish($msg, '', 'hello');
        $channel->basic_publish($msg1, '', 'hello');

        $channel->close();
        $connection->close();
        echo " [x] Sent two messages \n";

    }

    public function actionSendTask() {

        $connection = new AMQPStreamConnection($this->_mq_dns['host'], $this->_mq_dns['port'], $this->_mq_dns['user'], $this->_mq_dns['password']);
        $channel = $connection->channel();
        $channel->queue_declare('task_queue', false, true, false, false);

        $data = implode(' ', array_slice($_SERVER['argv'], 1));
        if (empty($data)) $data = "hello world";
        $msg = new AMQPMessage($data, ['delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT]);
        $channel->basic_publish($msg, '', 'task_queue');
        echo ' [x] Sent ', $data, "\n";
        $channel->basic_publish($msg, '', 'task_queue');

        $channel->close();
        $connection->close();

    }

    public function actionWorker() {

        $connection = new AMQPStreamConnection($this->_mq_dns['host'], $this->_mq_dns['port'], $this->_mq_dns['user'], $this->_mq_dns['password']);
        $channel = $connection->channel();
        $channel->queue_declare('task_queue', false, true, false, false);

        echo " [*] Waiting for messages. To exit press CTRL+C\n";

        $callback = function ($msg) {
            echo ' [x] Received ', $msg->body, "\n";
            sleep(substr_count($msg->body, '.'));
            echo " [x] Done\n";
            $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
        };
        $channel->basic_qos(null, 1, null);
        $channel->basic_consume('task_queue', '', false, false, false, false, $callback);
        while ($channel->is_consuming()) {
            $channel->wait();
        }

        $channel->close();
        $connection->close();

    }




    public function actionReceive() {

        $connection = new AMQPStreamConnection('localhost', 5672, 'admin', '123456');
        $channel = $connection->channel();
        $channel->queue_declare('hello', false, false, false, false);

        $callback = function($msg) {
            echo ' [x] Received ', $msg->body, "\n";
        };
        $channel->basic_consume('hello', '', false, true, false, false, $callback);

        while ($channel->is_consuming()) {
            $channel->wait();
        }

        $channel->close();
        $connection->close();

    }

    public function actionEmitLog() {

        $connection = new AMQPStreamConnection('localhost', 5672, 'admin', '123456');
        $channel = $connection->channel();
        $channel->exchange_declare('logs', 'fanout', false, false, false);

        $data = implode(' ', array_slice($_SERVER['argv'], 1));
        if (empty($data)) {
            $data = "info: Hello World!";
        }
        $msg = new AMQPMessage($data);

        $channel->basic_publish($msg, 'logs');

        echo ' [x] Sent ', $data, "\n";

        $channel->close();
        $connection->close();

    }

    public function actionReceiveLog() {

        $connection = new AMQPStreamConnection('localhost', 5672, 'admin', '123456');
        $channel = $connection->channel();
        $channel->exchange_declare('logs', 'fanout', false, false, false);
        list($queue_name, ,) = $channel->queue_declare("", false, false, true, false);
        $channel->queue_bind($queue_name, 'logs');

        echo " [*] Waiting for logs. To exit press CTRL+C\n";

        $callback = function ($msg) {
            echo ' [x] ', $msg->body, "\n";
        };

        $channel->basic_consume($queue_name, '', false, true, false, false, $callback);

        while ($channel->is_consuming()) {
            $channel->wait();
        }

        $channel->close();
        $connection->close();
    }

    public function actionEmitLogDirect() {

        $connection = new AMQPStreamConnection('localhost', 5672, 'admin', '123456');
        $channel = $connection->channel();
        $channel->exchange_declare('direct_logs', 'direct', false, false, false);
        $severity = isset($_SERVER['argv'][1]) && !empty($_SERVER['argv'][1]) ? $_SERVER['argv'][1] : 'info';
        $data = implode(' ', array_slice($_SERVER['argv'], 2));
        if (empty($data)) $data = 'hello world!';

        $msg = new AMQPMessage($data);
        $channel->basic_publish($msg, 'direct_logs', $severity);

        echo " [x] Sent ",$severity,':',$data," \n";

        $channel->close();
        $connection->close();
    }

    public function actionReceiveLogsDirect() {

        $connection = new AMQPStreamConnection('localhost', 5672, 'admin', '123456');
        $channel = $connection->channel();
        $channel->exchange_declare('direct_logs', 'direct', false, false, false);
        list($queue_name, ,) = $channel->queue_declare("", false, false, true, false);

        $severities = array_slice($_SERVER['argv'], 1);
        if (empty($severities )) {
            file_put_contents('php://stderr', "Usage: {$_SERVER['argv']} [info] [warning] [error]\n");
            exit(1);
        }
        foreach ($severities as $severity) {
            $channel->queue_bind($queue_name, 'direct_logs', $severity);
        }
        echo ' [*] Waiting for logs. To exit press CTRL+C', "\n";
        $callback = function($msg){
            echo ' [x] ', $msg->delivery_info['routing_key'], ':', $msg->body, "\n";
        };
        $channel->basic_consume($queue_name, '', false, true, false, false, $callback);

        while(count($channel->callbacks)) {
            $channel->wait();
        }

        $channel->close();
        $connection->close();
    }

}
