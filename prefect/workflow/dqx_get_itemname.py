from prefect import flow
from prefect import task
from prefect_github import GitHubCredentials
from prefect_github.repository import GitHubRepository
from prefect_github.repository import query_repository
#from prefect_github.mutations import add_star_starrable


@flow(log_prints=True)
def dqx_get_itemname(repo_name: str = "dqx-get-itemname"):
    github_repository_block = GitHubRepository.load("github-dqx-get-itemname")


if __name__ == "__main__":
    dqx_get_itemname.serve(name = "github-dqx-get-itemname")
