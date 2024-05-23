from Crypto.Cipher import AES


class PyAES:
    
    def __init__(self,
                 service,
                 SECRET_KEY,
                 IV,
                 REPLACE_KEY = None,
                 IV_EXTERNAL = None,
                 SECRET_KEY_EXTERNAL = None,
                 ALGORITHM = 'aes-256-cbc',
                 BLOCK_SIZE = 16):
        self.get_key = {
            'gorela': self.get_key_gorela,
            'zoo': self.get_key_zoo
        }
        self.get_iv = {
            'gorela': self.get_iv_gorela,
            'zoo': self.get_iv_zoo
        }
        self.replace_key = REPLACE_KEY
        self.secret_key = SECRET_KEY
        self.iv = self.get_iv[service](IV)
        self.key = self.get_key[service]()
        self.iv_external = IV_EXTERNAL
        self.secret_key_external = SECRET_KEY_EXTERNAL
        self.algorithm = ALGORITHM
        self.block_siz = BLOCK_SIZE
    
    def pad(self, data):
        pad_ = self.block_size - len(data) % self.block_size
        return data + pad_ * chr(pad_)

    def unpad (self, padded):
        pad_ = ord(chr(padded[-1]))
        return padded[:-pad_]

    def get_key_gorela(self, external=False):
        FINAL_SECRET_KEY = self.secret_key[0:9] + self.replace_key + self.secret_key[9+len(self.replace_key):]

        if not external:
            key = f"{FINAL_SECRET_KEY: <32}"
        else:
            key = f"{self.secret_key_external: <32}"
            # print([key, iv])
        return key.encode('utf-8')
    
    def get_key_zoo(self):
        key = bytes(self.secret_key, 'utf-8')
        return key

    def get_iv_gorela(self, iv):
        return bytes.fromhex(iv)

    def get_iv_zoo(self, iv):
        return bytes(iv, 'utf-8')


    def decrypt(self, data):
        if not data:
            return data
        try:
            aes = AES.new(self.key, AES.MODE_CBC, self.iv)
            return self.unpad(aes.decrypt(bytes.fromhex(data))).decode('utf-8')
        except Exception as e:
            print('encrypted data:', data)
            return 'ecryption error'
