# !/usr/bin/env python
import pika
import json
import config
import utilities
from process_video import ProcessVideo

s3 = utilities.create_s3_session(config.ACCESS_KEY, config.SECRET_KEY)

connection = pika.BlockingConnection(pika.URLParameters(config.RABBIT_MQ_CONNECTION_STRING))
channel = connection.channel()
queue =  f"{config.LISTEN_QUEUE}-{config.DO_TASK}"
channel.queue_declare(queue=queue, durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')


def inform_core(channel, method, result, collab_id, new_file_name, new_thumbnail):
    if result and not config.debug:
        message = {
                type: config.TASK_REPLY_FOR_TASK,
                status: result?0:1,
                message: result?"Success":"Fail",
                payload:{
                            "task_id": collab_id,
                            "new_file_name": new_file_name,
                            "new_thumbnail": new_thumbnail
                        }
                }
        channel.basic_ack(delivery_tag=method.delivery_tag)
        channel.basic_publish(exchange='', routing_key=f"{config.TELL_CORE_QUEUE}", body=json.dumps(message))
        return
    channel.basic_nack(delivery_tag=method.delivery_tag)


def callback(ch, method, properties, body):
    print(" [x] Received Task")
    body = json.loads(body)
    task_id = body['task_id']
    file_name = body['task_data']['unique_name']
    bucket = body['task_data']['bucket']

    process_obj = ProcessVideo(s3, task_id, file_name, bucket)
    result, video_id, thumbnail_id = process_obj.process()
    print(" [x] Done")
    inform_core(ch, method, result, task_id, video_id, thumbnail_id)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback, queue=queue)
channel.start_consuming()
