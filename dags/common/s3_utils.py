from io import BytesIO

import boto3

from common.string_utils import get_random_id


def upload_mpl_figure_to_s3(fig, filename, bucket_name, prefix='', profile_name=None, bbox_inches=None):
    # get a file-like object handle
    img_data = BytesIO()
    fig.savefig(img_data, format='png', bbox_inches=bbox_inches)
    img_data.seek(0)  # rewind the data
    # upload the image
    session = boto3.session.Session(profile_name=profile_name)  # Use profile_name='sandbox' for local testing
    s3_client = session.client('s3')
    rnd = get_random_id(length=24)
    key = '{prefix}_{rnd}_{filename}'.format(prefix=prefix, rnd=rnd, filename=filename)
    s3_client.put_object(Body=img_data, Bucket=bucket_name, Key=key, ACL='public-read')
    url = 'https://s3.amazonaws.com/{bucket_name}/{key}'.format(bucket_name=bucket_name, key=key)
    return url





