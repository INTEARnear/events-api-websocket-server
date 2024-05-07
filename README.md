# events-api-websocket-server

This repository serves data from [all-indexers](https://github.com/INTEARnear/all-indexers) to public Websocket API for getting realtime event. Goes well with [events-api-http-server](https://github.com/INTEARnear/events-api-http-server) for getting historical events.

The public API is hosted at wss://ws-events.intear.tech/

WebSocket endpoints:

- `/v0/nft/nft_mint`, optional message `{"token_account_id": <string>, "account_id": <string>}`: Get NFT mint events. All query parameters are optional. `token_account_id` is an account id of the NFT contract. `account_id` is an account id of the minter.
- `/v0/nft/nft_transfer`, optional message `{"token_account_id": <string>, "old_owner_id": <string>, "new_owner_id": <string>, "involved_account_ids": <string>}`: Get NFT transfer events. All query parameters are optional. `token_account_id` is an account id of the NFT contract. `old_owner_id` and `new_owner_id` are account ids of the old and new owners of the token. `involved_account_ids` is a comma-separated list of account ids that are involved in the transfer. With this parameter, `old_owner_id` and `new_owner_id` are ignored.
- `/v0/nft/nft_burn`, optional message `{"token_account_id": <string>, "account_id": <string>}`: Get NFT burn events. All query parameters are optional. `token_account_id` is an account id of the NFT contract. `account_id` is an account id of the wallet that burned the token.
- `/v0/potlock/potlock_donation`, optional message `{"project_id": <string>, "donor_id": <string>}`: Get Potlock donation events. All query parameters are optional. `project_id` is an account id of the project you want to filter by. `donor_id` is an account id of the account that donated.
- `/v0/potlock/potlock_pot_project_donation`, optional message `{"pot_id": <string>, "project_id": <string>, "donor_id": <string>}`: Get Potlock Pot Project donation events. All query parameters are optional. `pot_id` is an account id that ends with `.v1.potfactory.potlock.near`, `project_id` is an account id of the project you want to filter by. `donor_id` is an account id of the account that donated.
- `/v0/potlock/potlock_pot_donation`, optional message `{"pot_id": <string>, "donor_id": <string>}`: Get Potlock Pot donation events. All query parameters are optional. `pot_id` is an account id that ends with `.v1.potfactory.potlock.near`. `donor_id` is an account id of the account that donated.
- `/v0/trade/trade_pool`, optional message `{"pool_id": <string>, "account_id": <string>}`: Get raw pool swap events. All query parameters are optional. `pool_id` is a string in format `REF-<number>`. `account_id` is an account id of the trader.
- `/v0/trade/trade_swap`, optional message `{"involved_token_account_ids": <string>, "account_id": <string>}`: Get swap events, contains all raw pool swap events and net balance changes. All query parameters are optional. `involved_token_account_ids` is an account id of the token contract. Can contain multiple (usually you'd want 1 or 2) comma-separated values to filter by all these tokens. `account_id` is an account id of the trader.
- `/v0/trade/trade_pool_change`, optional message `{"pool_id": <string>}`: Get pool change events, when someone swaps, adds/removes liquidity, etc. All query parameters are optional. `pool_id` is a string in format `REF-<number>`.


Query parameters:

- `start_block_timestamp_nanosec` is the time after which you want to get events
- `blocks` is the number of unique blocks you want to retrieve events from, max 50.
- Other query parameters are filters.

The pagination is done by blocks, not events, so that it's easier for client libraries to paginate if a single block has hundreds of events. It skips blocks that contain no events. For example, if you set `blocks=3`, the server will return block 118058295 which contains 1 event, block 118058296 that contains 1 event, and block 118058299 that contains 2 events, so you will receive 4 events in total, as one array. After that, you can use `${events[events.length - 1].block_timestamp_nanosec}` as the next `start_block_timestamp_nanosec` (don't forget to check if `events.length !== 0`) and it's guaranteed that you won't miss any events.

Example: https://events.intear.tech/v0/nft/nft_transfer?start_block_timestamp_nanosec=1714988307491111000&blocks=3&token_account_id=uwon.hot.tg

Currently, only NFT API is implemented, and the public API doesn't have full event history, which will be fixed soon.
