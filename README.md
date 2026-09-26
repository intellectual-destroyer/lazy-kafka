# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/intellectual-destroyer/lazy-kafka/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                          |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|-------------------------------------------------------------- | -------: | -------: | -------: | -------: | ------: | --------: |
| src/lazy\_kafka/\_\_init\_\_.py                               |        6 |        0 |        0 |        0 |    100% |           |
| src/lazy\_kafka/\_\_main\_\_.py                               |       73 |       35 |        6 |        0 |     51% |30-34, 46, 49, 52-70, 78-85, 110-112, 116, 120-122, 125-127 |
| src/lazy\_kafka/\_logging.py                                  |       14 |       14 |        0 |        0 |      0% |      3-30 |
| src/lazy\_kafka/\_types.py                                    |       10 |       10 |        4 |        0 |      0% |      1-16 |
| src/lazy\_kafka/cli/\_\_init\_\_.py                           |        3 |        0 |        0 |        0 |    100% |           |
| src/lazy\_kafka/cli/\_cli.py                                  |       35 |        2 |       10 |        2 |     91% |    39, 79 |
| src/lazy\_kafka/config.py                                     |       88 |        2 |       12 |        1 |     97% |     78-79 |
| src/lazy\_kafka/connect.py                                    |      106 |        8 |        2 |        0 |     93% |108-109, 154-155, 217-221 |
| src/lazy\_kafka/entry\_points.py                              |        4 |        1 |        0 |        0 |     75% |         7 |
| src/lazy\_kafka/plugin/\_\_init\_\_.py                        |       33 |        2 |        4 |        0 |     95% |    57, 61 |
| src/lazy\_kafka/plugins/\_\_init\_\_.py                       |        3 |        0 |        0 |        0 |    100% |           |
| src/lazy\_kafka/plugins/core\_kafka/\_\_init\_\_.py           |       27 |        9 |        0 |        0 |     67% |     29-37 |
| src/lazy\_kafka/plugins/core\_kafka/cli.py                    |       80 |       55 |        8 |        0 |     28% |38-58, 84-103, 129-130, 134-203 |
| src/lazy\_kafka/plugins/kafka\_connect/\_\_init\_\_.py        |       23 |        6 |        0 |        0 |     74% |     30-35 |
| src/lazy\_kafka/plugins/schema\_registry/\_\_init\_\_.py      |       23 |        6 |        0 |        0 |     74% |     28-33 |
| src/lazy\_kafka/plugins/schema\_registry/cli.py               |       23 |        7 |        0 |        0 |     70% |     60-73 |
| src/lazy\_kafka/registry.py                                   |       99 |       16 |       12 |        2 |     80% |45, 60-64, 68-\>75, 123, 143-144, 188-207 |
| src/lazy\_kafka/scripts/\_\_init\_\_.py                       |        0 |        0 |        0 |        0 |    100% |           |
| src/lazy\_kafka/scripts/topic\_schema\_consumer.py            |       24 |       24 |        4 |        0 |      0% |      1-56 |
| src/lazy\_kafka/scripts/topic\_schema\_consumer\_ex.py        |       31 |       31 |        4 |        0 |      0% |      1-98 |
| src/lazy\_kafka/scripts/topic\_schema\_consumer\_generator.py |       32 |       32 |        6 |        0 |      0% |      1-65 |
| src/lazy\_kafka/scripts/topic\_schema\_consumer\_n\_times.py  |       64 |       64 |       16 |        0 |      0% |     1-127 |
| src/lazy\_kafka/scripts/topic\_schema\_producer.py            |       48 |       48 |        2 |        0 |      0% |     1-140 |
| src/lazy\_kafka/theme.py                                      |        4 |        0 |        0 |        0 |    100% |           |
| src/lazy\_kafka/topic.py                                      |      234 |      111 |       40 |        1 |     49% |158-160, 163-165, 211, 248-250, 268-285, 298-315, 328-397, 406-408, 411-425, 436-453 |
| src/lazy\_kafka/utils.py                                      |        9 |        0 |        2 |        0 |    100% |           |
| src/lazy\_kafka/widgets/\_\_init\_\_.py                       |        0 |        0 |        0 |        0 |    100% |           |
| src/lazy\_kafka/widgets/\_status.py                           |       10 |        1 |        0 |        0 |     90% |        32 |
| src/lazy\_kafka/widgets/common.py                             |      153 |       89 |       22 |        3 |     38% |33-\>exit, 63, 68, 155-156, 159, 162, 165-168, 171-174, 177-180, 183-196, 199-201, 204, 207-208, 211-221, 224, 228-235, 238-239, 243-255, 265-269, 275-276, 286-317, 320-\>exit, 323-\>exit |
| src/lazy\_kafka/widgets/kconnect.py                           |       46 |       24 |        4 |        0 |     44% |34-47, 51, 69-73, 76-77, 81-86, 90, 96-103 |
| src/lazy\_kafka/widgets/registry.py                           |      143 |       67 |       16 |        0 |     50% |38-41, 45, 101-104, 111-120, 134-152, 167-169, 177-179, 183-189, 192, 204-205, 225-231, 235-238, 242-247, 251-258, 268-269, 272-274, 279, 284 |
| src/lazy\_kafka/widgets/switcher.py                           |       46 |       25 |       12 |        0 |     36% |80-84, 92, 101-108, 123-139 |
| src/lazy\_kafka/widgets/topic.py                              |       37 |       14 |        0 |        0 |     62% |43-44, 47-51, 54-55, 58-60, 63, 68 |
| src/lazy\_kafka/widgets/topic\_details.py                     |       78 |       40 |        4 |        0 |     46% |48-52, 56-60, 81-87, 90-91, 94-97, 108-129, 134-135 |
| tests/\_\_init\_\_.py                                         |        0 |        0 |        0 |        0 |    100% |           |
| tests/conftest.py                                             |       88 |       27 |        0 |        0 |     69% |21, 31, 37, 43, 96-99, 116-122, 129-133, 139-140, 155-159 |
| tests/test\_cli.py                                            |       71 |        0 |        2 |        0 |    100% |           |
| tests/test\_config.py                                         |      135 |        0 |        0 |        0 |    100% |           |
| tests/test\_connect.py                                        |      168 |        0 |        0 |        0 |    100% |           |
| tests/test\_plugin.py                                         |      100 |       13 |        8 |        3 |     85% |55, 58, 74, 77, 106, 109, 134-137, 158, 167-169, 177-179 |
| tests/test\_registry.py                                       |      149 |        0 |        0 |        0 |    100% |           |
| tests/test\_topic.py                                          |      193 |        1 |        0 |        0 |     99% |       309 |
| tests/test\_utils.py                                          |       36 |        0 |        0 |        0 |    100% |           |
| tests/test\_widgets.py                                        |       70 |        0 |        0 |        0 |    100% |           |
| **TOTAL**                                                     | **2619** |  **784** |  **200** |   **12** | **67%** |           |


## Setup coverage badge

Below are examples of the badges you can use in your main branch `README` file.

### Direct image

[![Coverage badge](https://raw.githubusercontent.com/intellectual-destroyer/lazy-kafka/python-coverage-comment-action-data/badge.svg)](https://htmlpreview.github.io/?https://github.com/intellectual-destroyer/lazy-kafka/blob/python-coverage-comment-action-data/htmlcov/index.html)

This is the one to use if your repository is private or if you don't want to customize anything.

### [Shields.io](https://shields.io) Json Endpoint

[![Coverage badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/intellectual-destroyer/lazy-kafka/python-coverage-comment-action-data/endpoint.json)](https://htmlpreview.github.io/?https://github.com/intellectual-destroyer/lazy-kafka/blob/python-coverage-comment-action-data/htmlcov/index.html)

Using this one will allow you to [customize](https://shields.io/endpoint) the look of your badge.
It won't work with private repositories. It won't be refreshed more than once per five minutes.

### [Shields.io](https://shields.io) Dynamic Badge

[![Coverage badge](https://img.shields.io/badge/dynamic/json?color=brightgreen&label=coverage&query=%24.message&url=https%3A%2F%2Fraw.githubusercontent.com%2Fintellectual-destroyer%2Flazy-kafka%2Fpython-coverage-comment-action-data%2Fendpoint.json)](https://htmlpreview.github.io/?https://github.com/intellectual-destroyer/lazy-kafka/blob/python-coverage-comment-action-data/htmlcov/index.html)

This one will always be the same color. It won't work for private repos. I'm not even sure why we included it.

## What is that?

This branch is part of the
[python-coverage-comment-action](https://github.com/marketplace/actions/python-coverage-comment)
GitHub Action. All the files in this branch are automatically generated and may be
overwritten at any moment.