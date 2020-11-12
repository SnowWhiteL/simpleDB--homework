package simpledb;

import java.util.HashMap;

public class LRUCache {

	
	private HashMap<PageId,Page> cache;
	private LRUList lruList;
	private int capacity;
	public LRUCache(int capacity)
	{
		this.capacity=capacity;
		lruList=new LRUList(capacity);
		cache=new HashMap<PageId,Page>(capacity);
	}
	public HashMap<PageId,Page> getcache()
	{
		return this.cache;
	}
	
	public int getcacheNum()
	{
		return cache.size();
	}
	public Page get(PageId pid)
	{
		if(cache.containsKey(pid)&&cache.get(pid)!=null)
		{
			if(lruList.remove(pid))
			{
				lruList.add(pid);
			}
			else lruList.add(pid);
		}
		return cache.get(pid);
	}
	public void put(PageId pid,Page p)
	{
		cache.put(pid, p);
		lruList.add(pid);
		if(lruList.getNum()>capacity)
		{
			PageId delete=lruList.getHeader();
			lruList.remove(delete);
			cache.remove(delete);
		}
	}
}
class LRUList{

	class Node{
		PageId pageid;
		Node next;
		public Node(PageId pid)
		{
			pageid=pid;
			next=null;
		}
		public PageId get()
		{
			return this.pageid;
		}
	}
	private Node header;
	private Node tail;
	private int num;
	public LRUList(int capacity){
		num=0;
		header=null;
		tail=null;
		
	}
	public int getNum()
	{
		return num;
	}
	public void add(PageId pid)
	{
		Node newNode=new Node(pid);
		if(header==null)
		{
			header=newNode;
			tail=newNode;
		}
		else
		{
			tail.next=newNode;
			tail=newNode;
		}
		num++;
	}
	public boolean remove(PageId pid)
	{
		if(header==null)
		{
			return false;
		}
		
		num--;
		if(header.get()==pid)
		{Node temp=header;
			
			header=header.next;
			temp=null;
		return true;
		}
		else {
			Node temp=header;
			if(temp.next==null)
			{
				return false;
			}
			while(temp.next.get()!=pid&&temp.next.next!=null)
			{
				temp=temp.next;
			}
			
			if(temp.next.next==null)//最后一个
			{
				if(temp.next.get()==pid)
				{
					temp.next=null;
					return true;
				}
				else if(temp.next.get()!=pid)
				{
					return false;
				}
			}
			else 
			{
				Node cur=temp.next;
				temp.next=cur.next;
				cur=null;
				return true;
				
			}
		}
		System.out.println("end to end");
		return false;
		///////
		////////
	}
	public PageId getHeader()
	{
		return header.pageid;
	}
}