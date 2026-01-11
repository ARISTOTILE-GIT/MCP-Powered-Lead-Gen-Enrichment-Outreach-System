import asyncio
from aiosmtpd.controller import Controller

class CustomHandler:
    async def handle_DATA(self, server, session, envelope):
        print("\n" + "="*40)
        print(f"NEW EMAIL RECEIVED")
        print("="*40)
        print(f"FROM: {envelope.mail_from}")
        print(f"TO:   {envelope.rcpt_tos}")
        print("-" * 20)
        print("MESSAGE BODY:")

        try:
            content = envelope.content.decode('utf8', errors='replace')
            print(content)
        except Exception:
            print("[Binary Content]")
        print("="*40 + "\n")
        return '250 Message accepted for delivery'

if __name__ == '__main__':

    controller = Controller(CustomHandler(), hostname='localhost', port=1025)
    controller.start()
    print("Mock SMTP Server running on localhost:1025...")
    print("Waiting for emails... (Press Ctrl+C to stop)")
    
    try:
        asyncio.get_event_loop().run_forever()
    except KeyboardInterrupt:
        print("\nStopping server...")