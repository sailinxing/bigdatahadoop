package com.bigdata.hadoop.rpc.service;


import com.bigdata.hadoop.rpc.protocol.IUserLoginService;

public class UserLoginServiceImpl implements IUserLoginService {
//第二个rpc server
	@Override
	public String login(String name, String passwd) {
		
		return name + "logged in successfully...";
	}
	
	
	

}
