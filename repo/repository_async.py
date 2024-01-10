import asyncio
from .repository import Repository, Program, Table, OFMLPart, NotAvailable


class ProgramAsync(Program):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.collected_files_to_read = []

    async def load_ocd(self):
        if self.contains_ocd():
            res = await asyncio.to_thread(super().load_ocd)
            if type(res) is OFMLPart:
                self.on_ofml_part_loaded(res)

    async def load_oam(self):
        if self.contains_oam():
            res = await asyncio.to_thread(super().load_oam)
            if type(res) is OFMLPart:
                self.on_ofml_part_loaded(res)

    async def load_oas(self):
        if self.contains_oas():
            res = await asyncio.to_thread(super().load_oas)
            if type(res) is OFMLPart:
                self.on_ofml_part_loaded(res)

    async def load_go(self):
        if self.contains_go():
            res = await asyncio.to_thread(super().load_go)
            if type(res) is OFMLPart:
                self.on_ofml_part_loaded(res)

    async def load_oap(self):
        if self.contains_oap():
            res = await asyncio.to_thread(super().load_oap)
            if type(res) is OFMLPart:
                self.on_ofml_part_loaded(res)

    def on_ofml_part_error(self, err: NotAvailable):
        pass

    def on_ofml_part_loaded(self, ofml_part: OFMLPart):
        for name in ofml_part.filenames:

            self.collected_files_to_read.append(asyncio.to_thread(ofml_part.read_table, name))

    async def load_all(self):
        return await asyncio.gather(
            self.load_ocd(),
            self.load_oam(),
            self.load_oas(),
            self.load_go(),
            self.load_oap()
        )


class RepositoryAsync(Repository):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.collected_files_to_read = []

    async def read_profiles(self):
        return await asyncio.to_thread(super().read_profiles)

    async def load_program(self, program, keep_in_memory: bool = True, program_cls=None, **kwargs) -> ProgramAsync:
        result: ProgramAsync = await asyncio.to_thread(super().load_program,
                                                       **{
                                                           "program": program,
                                                           "keep_in_memory": keep_in_memory,
                                                           "program_cls": ProgramAsync
                                                       })

        callback = kwargs.get("on_done", None)
        if callback:
            callback(result)

        await self.on_program_loaded(result)
        return result

    async def on_program_loaded(self, program: ProgramAsync):
        await program.load_all()
        await asyncio.gather(*program.collected_files_to_read)

    def on_table_loaded(self, table: Table):
        ...
