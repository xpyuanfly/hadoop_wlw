package cn.hut.mr.friends;

import java.io.IOException;



public class PubFriend {

	public static void main(String[] args) {
		String inPathStep1="e:/friendInput/*";
		String outPathStep1="e:/friendsOutput/out1";
		String inPathStep2="e:/friendsOutput/out1/*";
		String outPathStep2="e:/friendsOutput/out2";
		try {
			if(PubFriendstep1.startStep1(inPathStep1,outPathStep1)&&PubFriendstep2.startStep2(inPathStep2,outPathStep2))
				System.out.println("���гɹ�");
			else {
				System.out.println("����ʧ��");
			}
		} catch (ClassNotFoundException | IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
