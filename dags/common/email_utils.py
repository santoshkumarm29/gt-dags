"""Helpers for sending emails via smtp.

"""
from airflow.utils.email import send_mime_email, get_email_address_list
from airflow.models import Variable
from airflow import configuration
import boto3
from email.header import Header
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.utils import formatdate
from common.string_utils import get_random_id
import logging
import mimetypes
import os
import smtplib
import traceback

from common.deprecated import deprecated


@deprecated
def send_mail_with_attachments(
        body,
        receivers,
        subject,
        sender="etl@gsngames.com",
        attachments=None,
        image_dct=None,
        smtp_host='localhost',
        smtp_port=25,
        smtp_username=None,
        smtp_password=None,
        verbose=False,
):
    """DEPRECATED:  Email should be sent using AWS SES instead of SMTP.
    Use `send_email_ses` instead.
    """
    msg_root = construct_raw_message(body, receivers, subject, sender=sender, attachments=attachments, image_dct=image_dct)
    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
        if verbose:
            server.set_debuglevel(True)
        # identify ourselves, prompting server for supported features
        server.ehlo()

        # If we can encrypt this session, do it
        if server.has_extn('STARTTLS'):
            server.starttls()
            server.ehlo()  # re-identify ourselves over TLS connection

        if smtp_username:
            server.login(smtp_username, smtp_password)
        server.sendmail(sender, receivers, msg_root.as_string())
    except smtplib.SMTPException as e:
        logging.getLogger().exception('error connecting to {}:{}'.format(smtp_host, smtp_port))
        raise e
    finally:
        server.quit()


def send_email_backend(to, subject, html_content, files=None, dryrun=False, cc=None, bcc=None, mime_subtype='mixed', mime_charset='utf-8', **kwargs):
    """Wrapper for 'send_email'
    Used for the 'email_backend' setting in Airflow config
    """
    smtp_mail_from = configuration.conf.get('smtp', 'SMTP_MAIL_FROM')
    send_email(source=smtp_mail_from, to_addresses=to, subject=subject, message=html_content, attachments=files, image_dct=None)

def send_email(source='', subject='', message='', to_addresses=[], cc_addresses=[], bcc_addresses=[], attachments=None, image_dct=None):
    """Send an email using SES.
    
    TODO:  Make this the default entry point for Airflow tasks sending emails to recipients.
    To do that during the migration from legacy Airflow to ECS Airflow, we need legacy jobs
    to be able to send email via SES and easily switch to the new Airflow sending email via
    SMTP.  We can add a try/except block here to try to first send via SMTP, then if that fails
    try sending via SES.
    """

    store_message_on_s3(subject, message)
#    try:
#        response = send_email_smtp(
#            from_email=source,
#            to=to_addresses,
#            subject=subject,
#            html_content=message,
#            files=attachments,
#        )
#    except:
#        logging.error('Failed to send email with SMTP. Falling back to SES.')
#        logging.error('Stack Trace: ' + traceback.format_exc())
    response = send_email_ses(
        source=source,
        to_addresses=to_addresses,
        cc_addresses=cc_addresses,
        bcc_addresses=bcc_addresses,
        subject=subject,
        message=message,
        attachments=attachments,
    )
    return response


def store_message_on_s3(subject, message):
    """stores email on s3

    uses GENERAL_S3_BUCKET_NAME/reports/raw-html-email/{subject}_{rnd}.txt
    :param subject: subject of the email, used as key
    :param message: message (e.g., html or plain text) to store
    """
    bucket_name = Variable.get("general_s3_bucket_name")
    session = boto3.session.Session()
    s3_client = session.client('s3')
    rnd = get_random_id(length=24)
    key = 'reports/raw-html-email/{subject}_{rnd}.txt'.format(subject=subject, rnd=rnd)
    s3_client.put_object(Body=message, Bucket=bucket_name, Key=key)


def send_email_smtp(from_email, to, subject, html_content, files=None):
    """Send an email with html content
    
    This is copied almost verbatim from airflow.utils.email except that it allows setting the sender.
    
    TODO: Find a simpler way to send emails, preferably with and email for humans library like `sender`.
    
    """
    SMTP_MAIL_FROM = from_email

    to = get_email_address_list(to)
    mime_subtype = 'mixed'
    msg = MIMEMultipart(mime_subtype)
    msg['Subject'] = subject
    msg['From'] = SMTP_MAIL_FROM
    msg['To'] = ", ".join(to)
    recipients = to
    # if cc:
    #     cc = get_email_address_list(cc)
    #     msg['CC'] = ", ".join(cc)
    #     recipients = recipients + cc
    # 
    # if bcc:
    #     # don't add bcc in header
    #     bcc = get_email_address_list(bcc)
    #     recipients = recipients + bcc

    msg['Date'] = formatdate(localtime=True)
    mime_text = MIMEText(html_content, 'html')
    msg.attach(mime_text)

    for fname in files or []:
        basename = os.path.basename(fname)
        with open(fname, "rb") as f:
            part = MIMEApplication(
                f.read(),
                Name=basename
            )
            part['Content-Disposition'] = 'attachment; filename="%s"' % basename
            part['Content-ID'] = '<%s>' % basename
            msg.attach(part)
    try:
        send_mime_email(SMTP_MAIL_FROM, recipients, msg)
    except smtplib.SMTPServerDisconnected as e:
        logging.error("Failed to send SMTP email.  from_email: {}, to: {}, subject: {}"
                      .format(from_email, to, subject))
        raise e


# def send_email_smtp_debug(from_email, to, subject, html_content, files=None):
#     """Send an email with html content
# 
#     This is copied almost verbatim from airflow.utils.email except that it allows setting the sender.
# 
#     TODO: Find a simpler way to send emails, preferably with and email for humans library like `sender`.
# 
#     """
#     # SMTP_MAIL_FROM = from_email
#     # DEBUG: send from the default
#     SMTP_MAIL_FROM = configuration.get('smtp', 'SMTP_MAIL_FROM')
#     to = get_email_address_list(to)
#     mime_subtype = 'mixed'
#     msg = MIMEMultipart(mime_subtype)
#     msg['Subject'] = subject
#     msg['From'] = SMTP_MAIL_FROM
#     msg['To'] = ", ".join(to)
#     recipients = to
#     # if cc:
#     #     cc = get_email_address_list(cc)
#     #     msg['CC'] = ", ".join(cc)
#     #     recipients = recipients + cc
#     # 
#     # if bcc:
#     #     # don't add bcc in header
#     #     bcc = get_email_address_list(bcc)
#     #     recipients = recipients + bcc
# 
#     msg['Date'] = formatdate(localtime=True)
#     mime_text = MIMEText(html_content, 'html')
#     msg.attach(mime_text)
# 
#     for fname in files or []:
#         basename = os.path.basename(fname)
#         with open(fname, "rb") as f:
#             part = MIMEApplication(
#                 f.read(),
#                 Name=basename
#             )
#             part['Content-Disposition'] = 'attachment; filename="%s"' % basename
#             part['Content-ID'] = '<%s>' % basename
#             msg.attach(part)
#     # send_MIME_email(SMTP_MAIL_FROM, recipients, msg)
#     #
#     # contents of send_MIME_email with modifications for debugging
#     #
#     
#     e_from = SMTP_MAIL_FROM
#     e_to = recipients
#     mime_msg = msg
#     dryrun = False
#     
#     log = LoggingMixin().log
# 
#     SMTP_HOST = configuration.get('smtp', 'SMTP_HOST')
#     SMTP_PORT = configuration.getint('smtp', 'SMTP_PORT')
#     SMTP_STARTTLS = configuration.getboolean('smtp', 'SMTP_STARTTLS')
#     SMTP_SSL = configuration.getboolean('smtp', 'SMTP_SSL')
#     SMTP_USER = None
#     SMTP_PASSWORD = None
# 
#     try:
#         SMTP_USER = configuration.get('smtp', 'SMTP_USER')
#         SMTP_PASSWORD = configuration.get('smtp', 'SMTP_PASSWORD')
#     except AirflowConfigException:
#         log.debug("No user/password found for SMTP, so logging in with no authentication.")
#     log.info('Using SMTP_HOST "{}"'.format(SMTP_HOST))
#     
#     if not dryrun:
#         s = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT) if SMTP_SSL else smtplib.SMTP(SMTP_HOST, SMTP_PORT)
#         s.set_debuglevel(1)
#         if SMTP_STARTTLS:
#             s.starttls()
#         if SMTP_USER and SMTP_PASSWORD:
#             s.login(SMTP_USER, SMTP_PASSWORD)
#         log.info("Sent an alert email to %s", e_to)
#         s.sendmail(e_from, e_to, mime_msg.as_string())
#         s.quit()


def send_email_ses(source='', subject='', message='', to_addresses=[], cc_addresses=[], bcc_addresses=[], attachments=None, image_dct=None):
    """Send an email using AWS SES.
    
    See: https://boto3.readthedocs.io/en/latest/reference/services/ses.html#SES.Client.send_email
    """
    session = boto3.session.Session()
    region_name = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    client = session.client('ses', region_name=region_name)
    charset = 'UTF-8'
    #logging.error('BODY:{}, RECEIVERS:{}, SUBJECT:{}, SENDER:{}, ATTCH:{}, IMG:{}'.format(message,to_addresses,subject,source,attachments,image_dct))
    if attachments is None and image_dct is None:
        response = client.send_email(
            Source=source,
            Destination={
                'ToAddresses': get_email_address_list(to_addresses),
                'CcAddresses': get_email_address_list(cc_addresses),
                'BccAddresses': get_email_address_list(bcc_addresses),
            },
            Message={
                'Subject': {
                    'Data': subject,
                    'Charset': charset
                },
                'Body': {
                    'Text': {
                        'Data': message,
                        'Charset': charset
                    },
                    'Html': {
                        'Data': message,
                        'Charset': charset
                    }
                }
            },
        )
    else:
        msg_root = construct_raw_message(body=message, receivers=to_addresses, cc_addresses=cc_addresses, bcc_addresses=bcc_addresses, subject=subject, sender=source, attachments=attachments, image_dct=image_dct)
        response = client.send_raw_email(
            RawMessage={
                'Data': msg_root.as_string()
            },
        )
    logging.getLogger().info('SES send_email response: {}'.format(response))
    return response


def construct_raw_message(body, subject, receivers=[], cc_addresses=[], bcc_addresses=[], sender="etl@gsngames.com", attachments=None, image_dct=None):
    mimetext_plain = MIMEText(body, 'plain', 'utf-8')
    mimetext_html = MIMEText(body, 'html', 'utf-8')

    # include the values of image_dct in the list of attachments
    if attachments:
        attachments = set(attachments)
    else:
        attachments = set()
    if image_dct:
        attachments = attachments.union(image_dct.values())

    if attachments:
        msg_root = MIMEMultipart('alternative')
        msg_root.attach(mimetext_plain)
        msg_root.attach(mimetext_html)
    else:
        msg_root = MIMEMultipart('mixed')
        msg_alt = MIMEMultipart('alternative')
        msg_alt.attach(mimetext_plain)
        msg_alt.attach(mimetext_html)
        msg_root.attach(msg_alt)

    # enable inline images in html
    if image_dct:
        for key, filepath in image_dct.items():
            with open(filepath, 'rb') as fp:
                msg_image = MIMEImage(fp.read(), name=key)
                msg_image.add_header('Content-ID', '<{key}>'.format(key=key))
                msg_image.add_header("Content-Disposition", "inline", filename=os.path.basename(filepath))
                msg_root.attach(msg_image)

    msg_root['Subject'] = Header(subject, 'utf-8')
    msg_root['From'] = sender
    if len(receivers) > 0:
      msg_root['To'] = ', '.join(receivers)
    if len(cc_addresses) > 0:
      msg_root['CC'] = ', '.join(cc_addresses)
    if len(bcc_addresses) > 0:
      msg_root['BCC'] = ', '.join(bcc_addresses)

    if attachments:
        for filepath in attachments:
            mimetype, _encoding = mimetypes.guess_type(filepath)
            _major_type, mime_subtype = mimetype.split('/', 1)
            if mimetype in (mimetypes.types_map['.png'], mimetypes.types_map['.jpg'], mimetypes.types_map['.gif']):
                with open(filepath, 'rb') as fp:
                    part = MIMEImage(fp.read())
            elif mimetype in (mimetypes.types_map['.xls'],):
                with open(filepath, 'rb') as fp:
                    part = MIMEApplication(fp.read(), mime_subtype)
            else:
                with open(filepath, 'r', encoding='utf-8') as fp:
                    part = MIMEText(fp.read(), 'csv')
            part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(filepath))
            msg_root.attach(part)
    return msg_root
