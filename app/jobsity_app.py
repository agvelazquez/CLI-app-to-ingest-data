import click

@click.group(invoke_without_command=True)
@click.pass_context
def main(ctx):
    """
        jobsity-challenge app is a command line tool that take the CSV file with
        trips taken by different vehicles and calculate the weekly average by Region.\n

        A local instance of SQL sever Management Studio is required.\n

        In order to run the app you will need to:\n
            1. Complete the config.json file with your setup \n
            2. Run create_database to create DB and tables  \n
            3. Load file with load_file given in the input folder in the json file \n
            4. Calculate the weekly average using weekly_average \n

        To check the status of the file loaded in the DB use file_status

    """
    group_commands = ['create_database', 'load_file', 'weekly_average', 'table_status']

    if ctx.invoked_subcommand is None:
        # No command supplied
        # Inform user on the available commands when running the app

        click.echo("Specify one of the commands below")
        print(*group_commands, sep='\n')

    click.echo()

@main.command('create_database')
@click.pass_context
def create_database(ctx):
    """
        :   Create database in the local instance specified in the config.json.

    """
    from app.utils import dbengine as dbe
    from app.utils import database as db

    engine = dbe.engine_setup()
    db.create_database(engine)
    db_engine = db.change_database()
    db.create_schema(db_engine)
    db.create_tables(db_engine)

@main.command('load_file')
@click.pass_context
def load_file(ctx):
    """
        :   Load file with trips by region and datasource.
    """
    from app.utils import loader as ld
    ld.load()

@main.command('weekly_average')
@click.pass_context
def weekly_average(ctx):
    """
        :   Calculate weekly average trips by Region.
    """
    from app.utils import weekly_average as wa
    df = wa.calculate_weekly_average()
    wa.create_visualization(df)

@main.command('table_status')
@click.pass_context
def files_status(ctx):
    """
        :   Check file status in the DB.
    """
    from app.utils import logger
    logger.table_status()

if __name__ == '__main__':
    main(obj={})