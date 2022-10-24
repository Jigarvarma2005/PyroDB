from pyrogram import Client
from pyrogram.types import Message
import json
from pyrogram.enums import ChatType
from pyrogram.errors import FloodWait
import asyncio
import logging

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
        #if not isinstance(bot, Client):
        #    raise TypeError("bot must be a Pyrogram Client")
        #if  not self.bot.get_chat_member(chat_id, "me").can_send_messages:
        #    raise Exception("Bot is not admin in the chat")
        #if self.bot.get_chat(chat_id).type == ChatType.PRIVATE:
        #    raise Exception("Chat must be a group")

    def validate(self, data):
        """validate the entered data always"""
        def parse_data(data) -> dict:
            try:
                if isinstance(data, list):
                    new_data = []
                    for x in data:
                        if len(data) > 4090:
                            raise Exception("data must be less than 4090 characters")
                        if not isinstance(x, dict):
                            json.loads(x)
                        new_data.append(x)
                    data = new_data
                elif isinstance(data, str):
                    if len(data) > 4090:
                        raise Exception("data must be less than 4090 characters")
                    try:
                        data = json.loads(data)
                    except:
                        data = json.loads(json.dumps(data))
            except Exception as e:
                raise Exception(e)
            return data
        return parse_data(data)
    
    def dict_to_str(self, data):
        if isinstance(data, dict):
            return json.dumps(data)
        else:
            return data
    
    async def get_many(self, data: str, limit: int = 100, is_dev=False):
        data = self.validate(data)
        try:
            all_data = []
            async for message in self.bot.search_messages(chat_id=self.chat_id, query=self.dict_to_str(data), limit=limit):
                if not is_dev:
                    all_data.append(message.text)
                else:
                    all_data.append(message)
            return all_data
        except:
            return None
    
    async def get_one(self, data: str, is_dev=False):
        data = self.validate(data)
        try:
            async for msg in self.bot.search_messages(chat_id=self.chat_id, query=self.dict_to_str(data), limit=1):
                if msg:
                    if not is_dev:
                        return msg.text
                    else:
                        return msg
        except Exception as e:
            self.logger.exception(e)
            return None
    
    async def insert_one(self, data: str):
        data = self.validate(data)
        try:
            await self.bot.send_message(chat_id=self.chat_id, text=self.dict_to_str(data))
        except:
            return None
    
    async def insert_many(self, data: list):
        data = self.validate(data)
        try:
            insert_data = []
            for i in data:
                try:
                    msg = await self.bot.send_message(chat_id=self.chat_id, text=self.dict_to_str(i))
                    insert_data.append(msg.text)
                except FloodWait as e:
                    await asyncio.sleep(e.x)
        except:
            return None

    async def delete_one(self, data: str):
        data = self.validate(data)
        try:
            msg = await self.get_one(data, is_dev=True)
            await msg.delete()
        except:
            return None
    
    async def delete_many(self, data: str, limit: int = 100):
        data = self.validate(data)
        try:
            msg = await self.get_many(data, limit=limit, is_dev=True)
            for i in msg:
                await i.delete()
        except:
            return None
    
    async def update_one(self, data: str, new_data: str):
        try:
            msg = await self.get_one(data, is_dev=True)
            old_data = self.validate(msg.text)
            self.logger.info(old_data)
            new_data = self.validate(new_data)
            old_data.update(new_data)
            await msg.edit(old_data)
        except Exception as e:
            self.logger.exception(e)
            return None
    
    async def update_many(self, data: str, new_data: str, limit: int = 100):
        try:
            data = self.validate(data)
            msgs = await self.get_many(data, limit=limit, is_dev=True)
            for i in msgs:
                old_data = self.validate(i.text)
                old_data.update(self.validate(new_data))
                await i.edit(self.dict_to_str(old_data))
        except:
            return None