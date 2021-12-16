<?php
/**
 * 定时任务
 */

namespace Cron;

use Conf\JkdConf;
use Log\JkdLog;

class JkdCron
{

    public static function start($masterPid, $timerPidFile)
    {
        if (\Jkd\JkdPreventDuplication::check('CRON') || file_get_contents($timerPidFile) == $masterPid) {
            $config = JkdConf::get('crontab', false);
            $confArray = $config ? $config->toArray() : [];

            if (isset($confArray['is_start']) && $confArray['is_start'] == true) {
                unset($confArray['is_start']);
                file_put_contents($timerPidFile, $masterPid);

                //定时清除日志
                $confArray[] = [
                    'class' => '\Log\JkdDeleteFile',
                    'func' => 'delLogs',
                    'cronTime' => '0 0 * * *'
                ];

                \Swoole\Timer::tick(360000, function () {
                    gc_mem_caches();
                    JkdLog::channel('crontab', '回收 Zend Engine 内存管理器使用的内存');
                });

                $data = array_group($confArray ?? [], 'cronTime');
                self::startTimer($data);
            }
        }
    }


    private static function startTimer($data)
    {
        $parser = new JkdParser();
        $timerTimes = [];
        $timerTimeTasks = [];
        foreach ($data as $cronTime => $da) {
            if ($res = $parser->parse($cronTime)) {
                $timerTimes[$cronTime] = $res;
                $timerTimeTasks[$cronTime] = $da;
            }
        }

        if ($timerTimes && $timerTimeTasks) {
            \Swoole\Timer::tick(1000, function () use ($timerTimes, $timerTimeTasks, $parser) {
                $thisTime = time();
                foreach ($timerTimes as $cronTime => $timerTime) {
                    if ($parser->check($thisTime, $timerTime)) {
                        if (isset($timerTimeTasks[$cronTime])) {
                            foreach ($timerTimeTasks[$cronTime] as $timerTimeTask) {
                                $timerPid = \Swoole\Timer::after(1, function () use ($timerTimeTask, $thisTime) {
                                    $class = new $timerTimeTask['class']();
                                    $func = $timerTimeTask['func'];
                                    $class->$func();

//                                    echo date('Y-m-d H:i:s', $thisTime) . '开始：' . json_encode($timerTimeTask) . PHP_EOL;
                                });
                                $timerArr = \Swoole\Timer::info($timerPid);
                            }
                        }
                    }
                }
                $load = sys_getloadavg();
                JkdLog::channel('crontab', 'Cpu', $load[0]);
                JkdLog::channel('crontab', '内存', memory_get_usage());
            });
        }
    }

}
