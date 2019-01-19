package web_data;

public class Intbyte {
	
	public static void main(String[] args) throws Exception {
		String key = "12335678";
		int num = 233567811;
		String encrypt = DESUtil.encrypt(int2byte(num), key);
		String encrypt2 = DESUtil.encryptStr(num+"", key);
		System.out.println(encrypt);
		System.out.println(encrypt2);
		System.out.println(DESUtil.decryptStr(encrypt2, key));
		System.out.println(byte2int(DESUtil.decrypt(encrypt, key)));
	}
	
	public static int byte2int(byte[] byt){  
        return (int)(byt[0]&0xff | (byt[1]&0xff)<<8 | (byt[2]&0xff)<<16 | (byt[3]&0xff)<<24);       
    }  
    public static byte[] int2byte(int number){  
        byte[] byt = new byte[4];  
        byt[0] = (byte)(number&0xff);  
        byt[1] = (byte)(number>>8&0xff);  
        byt[2] = (byte)(number>>16&0xff);  
        byt[3] = (byte)(number>>24&0xff);  
        return byt;  
    }  
   
}
