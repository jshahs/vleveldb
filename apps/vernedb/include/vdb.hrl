%%MQTT database

%%Subscription data

-record(vdb_topics,{topic,
		   subscriberId }).

-record(vdb_store,{subscriberId,
                        vmq_msg }).


-record(vdb_users,{subscriberId,
			 status,
                         on_node,
			sessionId }).

-record(vmq_msg, {
          msg_ref,               
          routing_key,           
          payload,               
          retain=false,          
          dup=false,             
          qos,                   
          trade_consistency=false, 
          reg_view=vmq_reg_trie,   
          mountpoint,            
          persisted=false       
         }).
