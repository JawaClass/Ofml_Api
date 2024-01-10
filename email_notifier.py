from notifiers import get_notifier
from settings import email_config


def send(subject: str, message: str):
    notification_content = {
        "subject": subject,
        "message": message,
        "to": ["fabian.gruenwald@koenig-neurath.de"],
        "from": email_config["username"]
    }

    email_notifier = get_notifier("email")
    response = email_notifier.notify(raise_on_errors=True, **email_config, **notification_content)

    print("Email Sent? :: response.status", response.status)
    if response.errors:
        print(response.errors)
