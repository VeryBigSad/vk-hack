import logging
import asyncio
import time
from aiogram import Bot
from configs.settings import env_parameters
from core.db.models import Dispatcher, Post, User
from core.db import init
from core.middlewares import i18n


logger = logging.getLogger(__name__)
bot = Bot(env_parameters.TELEGRAM_BOT_TOKEN, parse_mode="HTML")


async def send_content_message(post: Post, user_id: int):
    try:
        if (
            not post.photo_file_id
            and not post.video_file_id
            and not post.sticker_file_id
            and post.text
        ):
            await bot.send_message(chat_id=user_id, text=post.text)
        elif post.sticker_file_id:  # sticker
            await bot.send_sticker(chat_id=user_id, sticker=post.sticker_file_id)
        elif post.photo_file_id:  # photo
            await bot.send_photo(
                chat_id=user_id, photo=post.photo_file_id, caption=post.text
            )
        elif post.video_file_id:  # video
            await bot.send_video(
                chat_id=user_id, video=post.video_file_id, caption=post.text
            )
        else:
            logger.error(f"Unexpected content type: post_id={post.id}")

    except Exception as e:
        logger.error(
            f"Content sending error: user_id={user_id}, post_id={post.id}", exc_info=e
        )


async def order_work(order: Dispatcher):
    try:
        post = await Post.filter(id=(await order.post).id).first()
    except Exception as e:
        logger.error(f"Get post error", exc_info=e)
        return

    if order.user_id:  # send post by user_id
        try:
            await send_content_message(post=post, user_id=order.user_id)
            await asyncio.sleep(env_parameters.SLEEP_AFTER_SEND_CONTENT)
        except Exception as e:
            logger.error(f"Send content error to user_id={order.user_id}", exc_info=e)
            return

    # update or delete order
    try:
        await Dispatcher.update_or_delete_order(current_order=order, current_post=post)
    except Exception as e:
        logger.error(f"Update or delete order error", exc_info=e)
        return

    logger.info(
        f"Post step={post.step} in order_id={order.id} has been sent to user_id={order.user_id}"
    )


class Broadcaster(object):
    @classmethod
    async def start_event_loop(cls):
        logger.info("Broadcaster started")
        while True:
            try:
                active_orders = await Dispatcher.get_active_orders(
                    time.time()
                )  # depends on checking .sleep()
                logger.info(f"active_orders: {active_orders}")

            except Exception as e:
                logger.error(f"get active orders error", exc_info=e)
                continue

            index = 0
            futures = []
            try:
                if active_orders:
                    async with asyncio.TaskGroup() as tg:
                        while index < len(active_orders) or futures:
                            # start order work
                            if (
                                len(futures) < env_parameters.BROADCASTER_BATCH_SIZE
                            ) and (index < len(active_orders)):
                                futures.append(
                                    tg.create_task(order_work(active_orders[index]))
                                )
                                logger.info(
                                    f"Create order task for user_id={active_orders[index].user_id} "
                                    f"order_id={active_orders[index].id} "
                                    f"post_id={active_orders[index].post_id} "
                                )
                                index += 1

                            ind_x = 0
                            for i, _f in enumerate(reversed(futures)):
                                if _f.done():
                                    _f.cancel()
                                    del futures[len(futures) - i - 1 + ind_x]
                                    ind_x += 1
                            await asyncio.sleep(0.5)
                await asyncio.sleep(0.5)

            except Exception as e:
                logger.error(
                    f"Broadcaster main loop async creating tasks error", exc_info=e
                )
                continue

            await asyncio.sleep(env_parameters.BROADCASTER_SLEEP)


async def main():
    await init()
    i18n  # need to be here for import highlight
    broadcaster_obj = Broadcaster()
    await broadcaster_obj.start_event_loop()


if __name__ == "__main__":
    asyncio.run(main())
