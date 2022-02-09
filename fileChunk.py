import os
from enum import Enum

class ChunkStatus(Enum):
    PLANNED = 0
    CREATED = 1
    UPLOADED = 2
    CLEANED = 3

class Chunk:
    def __init__(self, path, part_number, chunk_size_bytes=1073741824):
        self.path = path
        self.status = ChunkStatus.PLANNED
        self.size = int(chunk_size_bytes)
        self.part_number = part_number
        self.md5 = None  # local md5
        self.md5_bytes = None
        self.etag = None  # AWS md5 (unofficially)

    def create_chunk(self, reader, memory_size_bytes=104857600):
        # 1MiB * 1024KiB/1MiB * 1024 B/1KiB
        tmp_dir = os.path.dirname(reader.name)

        with open(self.path, 'wb') as writer:
            for ibytes in range(0, self.size, int(memory_size_bytes)):
                # figure out how much to read
                next_read_bytes = min(memory_size_bytes, (self.size - ibytes))
                # Grab the next bytes
                bitbytes = reader.read(next_read_bytes)
                # Write the bytes
                writer.write(bitbytes)

        # Get md5 of chunk
#        self.md5 = get_md5(self.path)
 #       self.md5_bytes = get_md5(self.path, returnHex=False)
        # change status
        self.status = ChunkStatus.CREATED

def callChunk(origin_path,tmp_dir):
 chunks = []
 try:
   # == Split & Upload Loop ==
   # Set chunk temporary directory
   if tmp_dir is None:
        tmp_dir = os.path.dirname(origin_path)
   file_size = os.path.getsize(origin_path)
   read_pos = 0
   chunk_part = 1
   chunk_size_bytes = 100 * 1024 * 1024
   chunk_basename = os.path.splitext(os.path.basename(origin_path))[0]
   with open(origin_path, 'rb') as reader:
    while read_pos < file_size:
      # Update chunk name (path)
      chunk_path = os.path.join(tmp_dir, '{}_chunk_{}'.format(chunk_basename, chunk_part))
      # Create a chunk
      chunk = Chunk(chunk_path, chunk_part, chunk_size_bytes=chunk_size_bytes)
      chunk.create_chunk(reader)
      chunks.append(chunk)
      print('uploading chunk {}'.format(chunk_part))
      #self.__upload_multipart_part(multipart_meta, chunk)
      # update read position and chunk part
      read_pos += chunk_size_bytes
      chunk_part += 1
 except:
   print("error")
    
#origin_path='/mnt/d/movieLense/raw/rating.csv'
#tmp_dir='/mnt/d/movieLense/chunk/'

#callChunk(origin_path,tmp_dir)
