import gzip
with open('/mnt/d/movieLense/raw/rating.csv', 'rb') as f_in, gzip.open('/mnt/d/movieLense/raw/rating.csv.gz', 'wb') as f_out:
    f_out.writelines(f_in)
