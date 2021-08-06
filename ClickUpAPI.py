from pprint import pprint
import requests
from requests import Response
from math import ceil
import os
from urllib.parse import urlparse, urljoin
from typing import Union, List
from pyrate_limiter import RequestRate, Duration, MemoryListBucket, Limiter

token = os.environ.get("ClickUpToken")
RESOURCE_URI = "https://api.clickup.com/api/v2"
RATE_LIMIT = 100


class ClickupClient():
    """
    Basic class to call the Clickup API
    """
    task_bool_opt = ["reverse", "subtasks", "include_closed"]
    task_list_opt = [
        "space_ids",
        "project_ids",
        "list_ids",
        "statuses",
        "assignees",
        "tags",
    ]
    limit_rate = RequestRate(RATE_LIMIT, Duration.MINUTE)
    limiter = Limiter(limit_rate)
    item = 'clickup'

    def __init__(self, token):
        """Inits the basic ClickupCLient instance
        Args:
            token (str): personal token, https://clickup.com/api for more info. OAuth2 flow is not implemented here
        """
        self.token = token
        self.session = requests.Session()
        self.session.headers.update(
            {"Authorization": self.token}
        )

    @limiter.ratelimit(item, delay=True)
    def _get_wrapper(self, url: str) -> Response:
        """Wrapper for GET calls controlling for API rate limiting
        Args:
            url (str): url used for GET call
        Raises:
            ValueError: raised if no response or error response are received
        Returns:
            Response: response recieved from the server
        """
        resp = self.session.get(url)
        if resp.status_code != 200:
            raise ValueError(f"Какие-то беды. Код ошибки: {resp.status_code}")
        return resp.json()

    def _construct_endpoint(self, endpoint: str) -> str:
        """Convert a relative path such as /user to a full URI based
        on the current RESOURCE setting.
        Args:
            endpoint (str): endpoint to use for constructing a URL
        Returns:
            str: full URI, i.e. "https://api.clickup.com/api/v2/user"
        """
        if urlparse(endpoint).scheme in ["http", "https"]:
            return endpoint  # url is already complete
        return urljoin(f"{RESOURCE_URI}/", endpoint.lstrip("/"))

    def get_user(self) -> dict:
        """Get the user on whose behalf API calls are made
        Returns:
            dict: user id, username and some other fields. For more details
            refer to 'Get Authorized user' section on https://clickup.com/api
        """
        url = self._construct_endpoint("/user")
        return self._get_wrapper(url)['user']

    def get_teams(self, id_only: bool = False) -> Union[str, List[dict]]:
        """Get the id of the first team or info on all teams
        currently authenticated user is part of.
        Args:
            id_only (bool, optional): Set to True if the only key you're interested in is 'id'.
            Defaults to False.
        Returns:
            str or list: id of the first team the user has access to if id_only=True.
            List of all dicts containing info on all teams available to the user otherwise.
            For more details refer to 'Get Authorized Teams' section on https://clickup.com/api
        """
        url = self._construct_endpoint("/team")
        res = self._get_wrapper(url)
        if id_only:
            return res['teams'][0]['id']
        else:
            return res['teams']

    def get_spaces(self, team_id: str, archived: bool = False) -> list:
        """Get the spaces available in the team specified by the team_id argument
        Args:
            team_id (str): id of the team we want to retrieve spaces for
            archived (bool, optional): whether to include archived workspaces in the results.
            Defaults to False.
        Returns:
            list: list of dicts containing info on all spaces in the specified team.
            For more details refer to 'Get Spaces' section on https://clickup.com/api
        """
        url = self._construct_endpoint(
            f"/team/{team_id}/space?{str(archived).lower()}")
        res = self._get_wrapper(url)
        return res['spaces']

    def get_space_by_name(self, team_id: str, name: str,
                          archived: bool = False) -> dict:
        """Fetch the space with a name specified in the 'name' argument
        Args:
            team_id (str): id of the team we want to retrieve spaces for
            archived (bool, optional): whether to include archived workspaces in the results.
            Defaults to False.
            name (str, optional): name of the space we want to retrieve. Defaults to None.
        Raises:
            ValueError: raised if no space with such name was found
        Returns:
            dict: contains various space attributes. For a complete list
            refer to 'Get Spaces' section on https://clickup.com/api
        """
        spaces = self.get_spaces(team_id, archived)
        for space in spaces:
            if space['name'] == name:
                return space
        else:
            raise ValueError(
                "К сожалению, спейса с таким именем не нашлось")

    def get_lists(self, space_id: str, archived: bool = False) -> list:
        """Get the list of lists contained in the workspace specified in the space_id argument
        Args:
            space_id (str): id of the space we want to retrieve lists for
            archived (bool, optional): whether to include archived lists in the results. Defaults to False.
        Returns:
            list: collection of dicts with info on all lists in the specified space.
            For more details refer to 'Get Folderless Lists' section on https://clickup.com/api
        """
        url = self._construct_endpoint(
            f"/space/{space_id}/list?{str(archived).lower()}")
        res = self._get_wrapper(url)
        return res['lists']

    def get_list_by_name_and_space_id(
            self,
            space_id: str,
            archived: bool = False,
            name: str = None) -> dict:
        """Fetch the list with name specified in the 'name' argument
        Args:
            space_id (str): id of the space we want to retrieve lists for
            archived (bool, optional): whether to include archived lists in the results. Defaults to False.
            Defaults to False.
            name (str, optional): name of the list we want to retrieve. Defaults to None.
        Raises:
            ValueError: raised if no list with such name is found
        Returns:
            dict: info on the specified list.
            For more details refer to 'Get List' section on https://clickup.com/api
        """
        lists = self.get_lists(space_id, archived)
        for lst in lists:
            if lst['name'] == name:
                return lst
        else:
            raise ValueError(
                "К сожалению, листа с таким именем не нашлось")

    def get_tasks_100(self,
                      team_id: str,
                      page: int = None,  # page contains no more than 100 tasks
                      order_by: str = None,  # [id, created, updated, due_date]
                      reverse: bool = None,
                      subtasks: bool = None,
                      space_ids: list = None,
                      project_ids: list = None,
                      list_ids: list = None,
                      statuses: list = None,
                      include_closed: bool = False,
                      tags: list = None,
                      assignees: list = None,
                      due_date_gt: int = None,  # posix time
                      due_date_lt: int = None,  # posix time
                      date_created_gt: int = None,  # posix time
                      date_created_lt: int = None,  # posix time
                      date_updated_gt: int = None,  # posix time
                      date_updated_lt: int = None,
                      ) -> dict:
        """
        Gets the tasks contained in the team specified in the team_id argument.
        For a complete description of the arguments please refer to 'Get Filtered Team Tasks'
        section on https://clickup.com/api
        Returns:
            dict: complete json response containing 100 or less tasks with all their attributes.
            For a full list and more details please refer to 'Get Filtered Team Tasks' section
            on https://clickup.com/api
        """
        # dropping arguments which are not query keywords
        args = self._arg_filter(locals(), opt_exclude=['team_id'])
        for i in self.task_bool_opt:
            if i in args:
                args[i] = str(args[i]).lower()
        # constructing a query string
        query_list=[]
        for i in args:
            if i in self.task_list_opt:
                query_list.append('&'.join([ str(i)+'[]=' + str(k) for k in args[i]]))
            else:
                query_list.append(str(i) + '=' + str(args[i]))
        # query_list = [
        #     f"{''.join([str(i) if i not in self.task_list_opt])}"
        #     f"={'&'.join([ str(i)+'[]=' + str(k) for k in args[i]]) if i in self.task_list_opt else args[i]}"
        #     for i in args]
        query_string = "&".join(query_list)
        print(query_string)
        # calling the API
        url = self._construct_endpoint(f"/team/{team_id}/task?{query_string}")
        return self._get_wrapper(url)

    def get_all_tasks(self, page_limit=-1, **kwargs) -> List[dict]:
        """get all tasks contained in the specified team.
        Accepts the same arguments as get_tasks_100.
        Args:
            page_limit (int, optional): Number of pages (of size 100 tasks) to retrieve. Defaults to -1.
        Returns:
            list: list of json responses from the server. for more details re content of each response
            please refer to 'Get Filtered Team Tasks' section on https://clickup.com/api
        """
        page_num = 0
        tasks = []
        chunk = self.get_tasks_100(page=page_num, **kwargs)
        while chunk['tasks'] and (page_limit < 0 or page_num < page_limit):
            page_num += 1
            tasks.append(chunk)
            chunk = self.get_tasks_100(page=page_num, **kwargs)
        return tasks

    def get_task(self, task_id:str) -> dict:
        """Get information on a single task
        Args:
            task_id (str): id of a task to be retrieved
        Returns:
            dict: all the attributes of a task.
            For more details refer to 'Get Task' section on https://clickup.com/api
        """
        url = self._construct_endpoint(f"/task/{task_id}")
        return self._get_wrapper(url)

    def get_custom_fields(self, list_id: str) -> dict:
        """Get the list of custom fields defined in the list specified in the list_id argument
        Args:
            list_id (str): id of the list to retrieve the custom fields for
        Returns:
            dict: contains all the info related to custom fields defined in the list.
            For more details refer to 'Get Custom Fields' section on https://clickup.com/api
        """
        url = self._construct_endpoint(f"/list/{list_id}/field")
        return self._get_wrapper(url)

    def get_time_in_status(self, task_ids: List[str]) -> dict:  #похоже, что по 100 отдает
        """Get the status history for task ids specified in the task_ids argument.
        Args:
            task_ids (List[str]): list of task_ids to get information about
        Returns:
            dict: contains the info on status changes related to a tasks whose ids serve as dict keys.
            For more details refer to 'Get Bulk Tasks' Time in Status' section on https://clickup.com/api
        """
        chunks = self._chunkifier(task_ids, 100)
        res=[]
        for chunk in chunks:
            query_list = ["task_ids=" + task_id for task_id in chunk]
            query = "&".join(query_list)
            url = self._construct_endpoint(
                f"task/bulk_time_in_status/task_ids/?{query}")
            res.append(self._get_wrapper(url))
        return res

    def get_tags(self, space_id: str) -> dict:
        """Get the list of tags for the worksace secified in the space_id argument

        Args:
            space_id (str): idof the workspace to retrieve the tags from

        Returns:
            dict: contains the info on tag name and associated colors.
            For more details refer to 'Tags' section on https://clickup.com/api
        """
        url = self._construct_endpoint(f"/space/{space_id}/tag")
        return self._get_wrapper(url)

    @staticmethod
    def _arg_filter(locals: dict, opt_exclude: list = None) -> list:
        """
            Returns a dict without keys specified in the opt_exclude argument
        """
        excluder = ['self', 'kwargs']
        if opt_exclude:
            excluder += opt_exclude
        filtered_args = {i: locals[i] for i in locals
                         if i not in excluder and locals[i] is not None}
        return filtered_args

    @staticmethod
    def _chunkifier(list_to_split, n):
        return [list_to_split[i:i+n] for i in range(0, len(list_to_split), n)]
