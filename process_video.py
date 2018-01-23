import shlex
import subprocess
import config


class ProcessVideo:
    def __init__(self, s3, task_id, unique_name, bucket_name):
        self.s3 = s3
        self.task_id = task_id
        self.incoming_video = unique_name
        self.bucket_name = bucket_name
        self.outgoing_video = f"d_{self.incoming_video}"
        self.outgoing_thumbnail = f"{self.outgoing_video}_gif_thumbnail"
        pass

    def process(self):
        try:
            if self.download_from_s3().convert_to_mp4().convert_mp4_to_gif().upload_to_s3():
                return True, self.outgoing_video, self.outgoing_thumbnail
            else:
                return False
        except:
            return False

    def download_from_s3(self):
        self.s3.Bucket(self.bucket_name).download_file(self.incoming_video, self.outgoing_video)
        return self

    def convert_to_mp4(self):
        # TODO add watermark to videos
        temp_name = f"c_{self.outgoing_video}"
        conversion = f'ffmpeg -y -i {self.outgoing_video} -vcodec libx264 -acodec aac -vf "scale=trunc(iw/2)*2:trunc(ih/2)*2" -movflags +faststart -f mp4 {temp_name}'
        args = shlex.split(conversion)
        subprocess.call(args)
        self.outgoing_video = temp_name
        return self

    def upload_to_s3(self, ):
        with open(self.outgoing_video, mode='rb') as video:
            self.s3.Bucket(self.bucket_name).put_object(Key=self.outgoing_video, Body=video)
        with open(self.outgoing_thumbnail, mode='rb') as thumbnail:
            self.s3.Bucket(self.bucket_name).put_object(Key=self.outgoing_thumbnail, Body=thumbnail)
        return True

    def convert_mp4_to_gif(self):
        palette = f"/tmp/palette_{config.SERVER_ID}.png"  # have the processor id in a multiple processor ssytem
        args = shlex.split(f'ffmpeg -i {self.outgoing_video} -vf palettegen -y {palette}')
        subprocess.call(args)
        args = shlex.split(f'ffmpeg -i {self.outgoing_video} -r 10 -i {palette} -lavfi paletteuse -y {self.outgoing_thumbnail}')
        subprocess.call(args)  # based on its return value an if could be set for error handling
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
