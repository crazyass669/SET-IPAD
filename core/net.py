"""SSL context กลาง — verify cert จริงด้วย certifi (แทน _create_unverified_context()
ที่กระจายอยู่หลายไฟล์) เพราะ Windows บางเครื่อง (โดยเฉพาะ python.org installer) หา CA
bundle ของระบบเองไม่เจอ ทำให้ context ปกติ verify fail ทั้งที่ cert ปลายทางถูกต้อง —
ใช้ certifi.where() ตรงๆ กันปัญหานั้นโดยไม่ต้องปิด verify ทิ้งทั้งหมด"""
import ssl

import certifi

_ctx = None


def ssl_context():
    global _ctx
    if _ctx is None:
        _ctx = ssl.create_default_context(cafile=certifi.where())
    return _ctx
