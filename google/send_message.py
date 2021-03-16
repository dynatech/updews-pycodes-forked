#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 18 09:50:05 2019

@author: brain
"""

"""Example of using hangups to send a chat message to a conversation."""

import hangups

from common import run_example


async def send_message(client, args):
    mes = hangups.ChatMessageSegment.from_str(args.message_text)
    segment_message = []
    for i in mes:
        segment_message.append(i.serialize())
        
    request = hangups.hangouts_pb2.SendChatMessageRequest(
        request_header=client.get_request_header(),
        event_request_header=hangups.hangouts_pb2.EventRequestHeader(
            conversation_id=hangups.hangouts_pb2.ConversationId(
                id=args.conversation_id
            ),
            client_generated_id=client.get_client_generated_id(),
        ),
        message_content=hangups.hangouts_pb2.MessageContent(
            segment=segment_message,
        ),
    )
    await client.send_chat_message(request)


if __name__ == '__main__':
    run_example(send_message, '--conversation-id', '--message-text')