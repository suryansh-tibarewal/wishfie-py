import shlex
import subprocess
import config
import traceback
import os


class ProcessVideo:
    def __init__(self, s3, task_id, unique_name, bucket_name):
        self.s3 = s3
        self.task_id = task_id
        self.incoming_video = unique_name
        self.bucket_name = bucket_name
        self.outgoing_video = f"d_{self.incoming_video}"
        self.outgoing_thumbnail = f"c_{self.outgoing_video}_thumbnail"
        pass

    def process(self):
        try:
            if self.download_from_s3().convert_to_mp4().convert_mp4_to_gif().upload_to_s3():
                os.remove(self.outgoing_video)
                os.remove(self.outgoing_thumbnail)
                return True, self.outgoing_video, self.outgoing_thumbnail
            else:
                return False, None, None
        except Exception as e:
            traceback.print_exc()
            return False, None, None

    def download_from_s3(self):
        self.s3.Bucket(self.bucket_name).download_file(self.incoming_video, self.outgoing_video)
        return self

    def convert_to_mp4(self):
        # TODO add watermark to videos
        temp_name = f"c_{self.outgoing_video}"
        conversion = f'ffmpeg -y -i {self.outgoing_video} -vcodec libx264 -acodec aac -vf "scale=trunc(iw/2)*2:trunc(ih/2)*2" -movflags +faststart -f mp4 {temp_name}'
        args = shlex.split(conversion)
        subprocess.call(args)
        os.remove(self.outgoing_video)
        self.outgoing_video = temp_name
        return self

    def upload_to_s3(self, ):
        with open(self.outgoing_video, mode='rb') as video:
            self.s3.Bucket(self.bucket_name).put_object(Key=self.outgoing_video, Body=video)
        with open(self.outgoing_thumbnail, mode='rb') as thumbnail:
            self.s3.Bucket(self.bucket_name).put_object(Key=self.outgoing_thumbnail, Body=thumbnail)
        return True

    def convert_mp4_to_gif(self):
        args_str = f"ffmpeg -ss 3 -t 3 -i {self.outgoing_video} -vf scale=80:-1 -r 4 -f image2pipe -vcodec ppm - | convert-im6.q16 -delay 100 -loop 0 - gif:- | sudo convert-im6.q16 -layers Optimize - gif:{self.outgoing_thumbnail}"
        ps = subprocess.Popen(args_str,shell=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        output = ps.communicate()[0]
        print(output)
        #print(args_str)
        #args = shlex.split(args_str)
        #subprocess.call(args_str, shell=True)
        return self

### extra info

# params for video to gif
# -r 10 for setting fps
# -ss 15 to set the starting time
# -t 20 to set the duration of gif
# -vf scale=160:90 to change the scale

# to get the format
# ffprobe -show_format -show_streams -loglevel quiet -print_format json "file_name"

# we are doing this
# obj = s3.Object('mybucket', 'hello.txt').download_file('/tmp/hello.txt')

# to stream
## .get()['Body'] returns a generator!
##fileobj = s3.Object('mybucket', 'hello.txt').get()['Body']
