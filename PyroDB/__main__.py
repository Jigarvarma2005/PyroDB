"""
(c) Jigarvarma2005

Simple database for pyrogram based bots.

Feel free to contribute.
"""


from typing import List
from pyrogram import Client
import json
from pyrogram.enums import ChatType, ChatMemberStatus
from pyrogram.errors import FloodWait
import asyncio
import logging
import uuid

class PyroDB:
    """Simple Database for Pyrogram based bots"""
    
    def __init__(self, bot: Client, chat_id: int):
        """Initialize the database
        
        bot: pyrogram client instance
        chat_id: ID of a group with admin privillages
        """
        self.bot = bot
        self.chat_id = chat_id
        self.logger = logging.getLogger(__name__)

        if not isinstance(bot, Client):
            raise TypeError("User must be a Pyrogram Client")
        try:
            bot.start()
        except:
            pass
        if  not self.bot.get_chat_member(chat_id, "me").status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER]:
            raise Exception("User is not admin in the chat")
        if self.bot.get_chat(chat_id).type == ChatType.PRIVATE:
            raise Exception("Chat must be a group")
        try:
            bot.stop()
        except:
            pass

    def validate(self, data, is_entry=False):
        """validate the entered data always"""
        def parse_data(data, is_entry) -> dict:
            try:
                if isinstance(data, list):
                    new_data = []
                    for x in data:
                        new_data.append(self.validate(x, is_entry))
                    data = new_data
                elif isinstance(data, str):
                    if len(data) > 4090:
                        raise Exception("data must be less than 4090 characters")
                    try:
                        data = json.loads(data)
                    except:
                        data = json.loads(json.dumps(data))
                    if is_entry and "_id" not in data:
                        data["_id"] = uuid.uuid4().hex
            except Exception as e:
                raise Exception(e)
            return data
        return parse_data(data, is_entry)

    def dict_to_str(self, data) -> str:
        """Convert dict to str"""
        if isinstance(data, dict):
            return json.dumps(data)
        else:
            return data
    
    async def get_many(self, data: dict, limit: int = 100, is_dev=False) -> List[dict]:
        """Get many reuslt of query
        
        args:
            data: query data
            limit: limit the query (defaults to 100)
            is_dev: return list of pyrogram message object instead dict
        
        return list of results"""
        data = self.validate(data)
        try:
            all_data = []
            async for message in self.bot.search_messages(chat_id=self.chat_id, query=self.dict_to_str(data), limit=limit):
                if not is_dev:
                    all_data.append(self.validate(message.text))
                else:
                    all_data.append(message)
            return all_data
        except:
            return None

    async def get_one(self, data: str, is_dev=False) -> dict:
        """Get first result of query
        
        args:
            data: query data
            is_dev: return pyrogram message object instead dict
        
        return dict"""
        data = self.validate(data)
        try:
            async for msg in self.bot.search_messages(chat_id=self.chat_id, query=self.dict_to_str(data), limit=1):
                if msg:
                    if not is_dev:
                        return self.validate(msg.text)
                    else:
                        return msg
        except Exception as e:
            self.logger.exception(e)
            return None
    
    async def insert_one(self, data: str) -> dict:
        """Insert one data
        args:
            data: dict of data

        returns dict"""
        data = self.validate(data, is_entry=True)
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=self.dict_to_str(data))
            return data
        except:
            return None
    
    async def insert_many(self, data: list) -> List[dict]:
        """insert many data at once
        
        args:
            data: list of dict data
        
        returns list of dict"""
        data = self.validate(data, is_entry=True)
        try:
            insert_data = []
            for i in data:
                try:
                    msg = await self.bot.send_message(chat_id=self.chat_id, text=self.dict_to_str(i))
                    insert_data.append(i)
                except FloodWait as e:
                    await asyncio.sleep(e.x)
                return insert_data
        except:
            return None

    async def delete_one(self, data: str) -> bool:
        """Delete an data
        
        args:
            data: key to delete
        
        returns bool"""
        data = self.validate(data)
        try:
            msg = await self.get_one(data, is_dev=True)
            await msg.delete()
            return True
        except:
            return False
    
    async def delete_many(self, data: str, limit: int = 100) -> bool:
        """Delete many data at once
        
        args:
            data: list of keys
        
        returns bool"""
        data = self.validate(data)
        try:
            msg = await self.get_many(data, limit=limit, is_dev=True)
            for i in msg:
                await i.delete()
            return True
        except:
            return False
    
    async def update_one(self, data: str, new_data: str) -> dict:
        """Update first query result with new data
        
        args:
            data: key to find the data
            new_data: new data to be updated with
        
        returns dict"""
        try:
            msg = await self.get_one(data, is_dev=True)
            old_data = self.validate(msg.text)
            new_data = self.validate(new_data)
            old_data.update(new_data)
            await msg.edit(self.dict_to_str(old_data))
            return old_data
        except Exception as e:
            self.logger.exception(e)
            return None
    
    async def update_many(self, data: str, new_data: str, limit: int = 100) -> List[dict]:
        """Update all query result with new data
        
        args:
            data: key to find the data
            new_data: new data to be updated with
        
        returns list of dict"""
        try:
            data = self.validate(data)
            msgs = await self.get_many(data, limit=limit, is_dev=True)
            update_data = []
            for i in msgs:
                old_data = self.validate(i.text)
                old_data.update(self.validate(new_data))
                await i.edit(self.dict_to_str(old_data))
                update_data.append(old_data)
            return update_data
        except:
            return []
