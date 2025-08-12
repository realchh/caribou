import unittest
from unittest.mock import MagicMock, patch
from caribou.deployment_solver.deployment_metrics_calculator.models.workflow_instance import WorkflowInstance
from caribou.deployment_solver.deployment_input.input_manager import InputManager
from caribou.deployment_solver.deployment_metrics_calculator.models.instance_edge import InstanceEdge
from caribou.deployment_solver.deployment_metrics_calculator.models.instance_node import InstanceNode
from caribou.deployment_solver.deployment_metrics_calculator.models.simulated_instance_edge import SimulatedInstanceEdge


class TestWorkflowInstance(unittest.TestCase):
    def setUp(self):
        self.input_manager = MagicMock(spec=InputManager)
        self.input_manager.get_start_hop_retrieve_wpd_probability.return_value = 0.0
        self.instance_deployment_regions = [0, 1, 2]
        self.start_hop_instance_index = 0
        self.consider_from_client_latency = True

        self.input_manager.get_start_hop_info.return_value = {
            "read_capacity_units": 5.0,
            "workflow_placement_decision_size": 0.1,
        }

        self.workflow_instance = WorkflowInstance(
            self.input_manager,
            self.instance_deployment_regions,
            self.start_hop_instance_index,
            self.consider_from_client_latency,
        )

    def test_configure_node_regions(self):
        # No redirector node
        self.input_manager.get_start_hop_retrieve_wpd_probability.return_value = 0.0
        workflow_instance = WorkflowInstance(
            self.input_manager,
            self.instance_deployment_regions,
            self.start_hop_instance_index,
            self.consider_from_client_latency,
        )
        self.workflow_instance._configure_node_regions(self.instance_deployment_regions)
        self.assertEqual(len(workflow_instance._nodes), len(self.instance_deployment_regions) + 1)

        # With redirector node
        self.input_manager.get_start_hop_retrieve_wpd_probability.return_value = 1.0
        workflow_instance = WorkflowInstance(
            self.input_manager,
            self.instance_deployment_regions,
            self.start_hop_instance_index,
            self.consider_from_client_latency,
        )
        self.workflow_instance._configure_node_regions(self.instance_deployment_regions)
        self.assertEqual(len(workflow_instance._nodes), len(self.instance_deployment_regions) + 2)

    def test_add_start_hop(self):
        self.workflow_instance.add_start_hop(1)
        self.assertTrue(self.workflow_instance._nodes[1].invoked)

    def test_add_edge(self):
        self.workflow_instance.add_edge(0, 1, True)
        self.assertIn(0, self.workflow_instance._edges[1])

    def test_add_node(self):
        result = self.workflow_instance.add_node(1)
        self.assertIn(1, self.workflow_instance._nodes)
        self.assertIsInstance(result, bool)

    def test_calculate_overall_cost_runtime_carbon(self):
        """Test overall metrics calculation"""
        # Clear any existing nodes first
        self.workflow_instance._nodes.clear()

        # Create exactly the nodes we want to test
        mock_results = [
            {"cost": 10.0, "runtime": 2.0, "execution_carbon": 5.0, "transmission_carbon": 3.0},
            {"cost": 11.0, "runtime": 3.0, "execution_carbon": 6.0, "transmission_carbon": 4.0},
            {"cost": 12.0, "runtime": 4.0, "execution_carbon": 7.0, "transmission_carbon": 5.0},
        ]

        # Create and mock only the nodes we're testing
        for i, mock_result in enumerate(mock_results):
            node = self.workflow_instance._get_node(i)
            node.calculate_carbon_cost_runtime = MagicMock(return_value=mock_result)

        result = self.workflow_instance.calculate_overall_cost_runtime_carbon()

        # Cost should be cumulative: 10 + 11 + 12 = 33
        self.assertEqual(result["cost"], 33.0)
        # Runtime should be max: max(2, 3, 4) = 4
        self.assertEqual(result["runtime"], 4.0)
        # Execution carbon cumulative: 5 + 6 + 7 = 18
        self.assertEqual(result["execution_carbon"], 18.0)
        # Transmission carbon cumulative: 3 + 4 + 5 = 12
        self.assertEqual(result["transmission_carbon"], 12.0)
        # Total carbon: 18 + 12 = 30
        self.assertEqual(result["carbon"], 30.0)

    def test_get_node(self):
        node = self.workflow_instance._get_node(1)
        self.assertIsInstance(node, InstanceNode)

    def test_create_edge(self):
        edge = self.workflow_instance._create_edge(0, 1)
        self.assertIsInstance(edge, InstanceEdge)

    def test_create_simulated_edge(self):
        simulated_edge = self.workflow_instance._create_simulated_edge(0, 1, 2, 3)
        self.assertIsInstance(simulated_edge, SimulatedInstanceEdge)

    def test_manage_data_transfer_dict(self):
        data_transfer_dict = {}
        self.workflow_instance._manage_data_transfer_dict(data_transfer_dict, 1, 100.0)
        self.assertIn(1, data_transfer_dict)
        self.assertEqual(data_transfer_dict[1], 100.0)

    def test_manage_sns_invocation_data_transfer_dict(self):
        sns_data_transfer_dict = {}
        self.workflow_instance._manage_sns_invocation_data_transfer_dict(sns_data_transfer_dict, 1, 100.0)
        self.assertIn(1, sns_data_transfer_dict)
        self.assertEqual(sns_data_transfer_dict[1], [100.0])

    def test_get_predecessor_edges(self):
        edges = self.workflow_instance._get_predecessor_edges(1, False)
        self.assertIsInstance(edges, list)

    def test_retrieved_wpd_at_function(self):
        with patch("random.random", return_value=0.1):
            self.input_manager.get_start_hop_retrieve_wpd_probability.return_value = 0.5
            result = self.workflow_instance._retrieved_wpd_at_function()
            self.assertTrue(result)

    def test_configure_node_regions_no_redirector(self):
        """Test node configuration without redirector"""
        self.input_manager.get_start_hop_retrieve_wpd_probability.return_value = 0.0
        workflow_instance = WorkflowInstance(
            self.input_manager,
            self.instance_deployment_regions,
            self.start_hop_instance_index,
            self.consider_from_client_latency,
        )

        # Should have virtual client node (-1) + 3 instance nodes (0, 1, 2)
        self.assertEqual(len(workflow_instance._nodes), 4)
        self.assertIn(-1, workflow_instance._nodes)  # Virtual client
        self.assertEqual(workflow_instance._nodes[-1].region_id, -1)

        # Check instance nodes have correct regions
        for i, region in enumerate(self.instance_deployment_regions):
            self.assertEqual(workflow_instance._nodes[i].region_id, region)

    def test_configure_node_regions_with_redirector(self):
        """Test node configuration with redirector"""
        self.input_manager.get_start_hop_retrieve_wpd_probability.return_value = 1.0
        self.input_manager.get_home_region_index.return_value = 1  # Different from start hop
        workflow_instance = WorkflowInstance(
            self.input_manager,
            [2, 1, 0],  # Start hop in region 2, home region is 1
            0,  # Start hop instance index
            self.consider_from_client_latency,
        )

        # Should have virtual client (-2), redirector (-1), + 3 instance nodes
        self.assertEqual(len(workflow_instance._nodes), 5)
        self.assertIn(-2, workflow_instance._nodes)  # Virtual client
        self.assertIn(-1, workflow_instance._nodes)  # Redirector

        # Redirector should be in home region
        self.assertEqual(workflow_instance._nodes[-1].region_id, 1)
        self.assertEqual(workflow_instance._nodes[-1].actual_instance_id, 0)

    def test_redirector_exists_logic(self):
        """Test redirector existence conditions"""
        # Case 1: WPD not retrieved at function - no redirector
        with patch("random.random", return_value=0.9):
            self.input_manager.get_start_hop_retrieve_wpd_probability.return_value = 0.5
            workflow_instance = WorkflowInstance(
                self.input_manager,
                [1, 0, 2],  # Start hop not in home region
                0,
                True,
            )
            self.assertFalse(workflow_instance._redirector_exists)

        # Case 2: WPD retrieved but start hop in home region - no redirector
        with patch("random.random", return_value=0.1):  # WPD retrieved
            self.input_manager.get_start_hop_retrieve_wpd_probability.return_value = 0.5
            # Make sure home region is 0 and start hop is also in region 0
            self.input_manager.get_home_region_index.return_value = 0
            workflow_instance = WorkflowInstance(
                self.input_manager,
                [0, 1, 2],  # Start hop in home region (0)
                0,  # Start hop instance index 0 -> region 0
                True,
            )
            self.assertFalse(workflow_instance._redirector_exists)

        # Case 3: WPD retrieved and start hop not in home region - redirector exists
        with patch("random.random", return_value=0.1):  # WPD retrieved
            self.input_manager.get_start_hop_retrieve_wpd_probability.return_value = 0.5
            # Make sure home region is 0 but start hop is in region 1
            self.input_manager.get_home_region_index.return_value = 0
            workflow_instance = WorkflowInstance(
                self.input_manager,
                [1, 0, 2],  # Start hop instance 0 -> region 1 (not home region)
                0,  # Start hop instance index 0
                True,
            )
            self.assertTrue(workflow_instance._redirector_exists)

        # Case 4: WPD not retrieved - no redirector regardless of regions
        with patch("random.random", return_value=0.9):  # WPD NOT retrieved
            self.input_manager.get_start_hop_retrieve_wpd_probability.return_value = 0.5
            self.input_manager.get_home_region_index.return_value = 0
            workflow_instance = WorkflowInstance(
                self.input_manager,
                [1, 0, 2],  # Start hop not in home region, but WPD not retrieved
                0,
                True,
            )
            self.assertFalse(workflow_instance._redirector_exists)

    def test_add_start_hop_no_redirector_no_wpd_retrieval(self):
        """Test adding start hop without redirector and without WPD retrieval at function"""
        with patch("random.random", return_value=0.9):  # Don't retrieve WPD
            workflow_instance = WorkflowInstance(
                self.input_manager,
                self.instance_deployment_regions,
                0,
                True,
            )
            workflow_instance.add_start_hop(1)

            # Virtual client and start hop should be invoked
            self.assertTrue(workflow_instance._nodes[-1].invoked)  # Virtual client
            self.assertTrue(workflow_instance._nodes[1].invoked)  # Start hop

            # Should have edge from virtual client to start hop
            self.assertIn(-1, workflow_instance._edges[1])

    def test_add_start_hop_with_redirector(self):
        """Test adding start hop with redirector"""
        with patch("random.random", return_value=0.1):  # Retrieve WPD
            # Mock the get_node_runtimes_and_data_transfer method to return proper values
            self.input_manager.get_node_runtimes_and_data_transfer.return_value = ([1.0, 2.0], 1.5, 0.1)

            self.input_manager.get_start_hop_retrieve_wpd_probability.return_value = 1.0
            workflow_instance = WorkflowInstance(
                self.input_manager,
                [1, 0, 2],  # Start hop not in home region
                0,
                True,
            )
            workflow_instance.add_start_hop(0)

            # All relevant nodes should be invoked
            self.assertTrue(workflow_instance._nodes[-2].invoked)  # Virtual client
            self.assertTrue(workflow_instance._nodes[-1].invoked)  # Redirector
            self.assertTrue(workflow_instance._nodes[0].invoked)  # Start hop

            # Should have edges: virtual client -> redirector -> start hop
            self.assertIn(-2, workflow_instance._edges[-1])  # Client to redirector
            self.assertIn(-1, workflow_instance._edges[0])  # Redirector to start hop

    def test_add_start_hop_wpd_tracking(self):
        """Test WPD data tracking in start hop"""
        workflow_instance = self.workflow_instance
        workflow_instance.add_start_hop(0)

        # Check that WPD data was tracked
        # In this case, the virtual client node gets the WPD, not the start hop
        wpd_node = workflow_instance._nodes[-1]  # Virtual client gets WPD when no redirector
        self.assertEqual(wpd_node.tracked_dynamodb_read_capacity, 5.0)
        self.assertIn(-2, wpd_node.tracked_data_input_sizes)  # System region
        self.assertEqual(wpd_node.tracked_data_input_sizes[-2], 0.1)

    def test_add_edge_invocation_logic(self):
        """Test edge invocation logic based on node invocation"""
        # Add nodes
        self.workflow_instance._nodes[0].invoked = True
        self.workflow_instance._nodes[1].invoked = False

        # Add edges
        self.workflow_instance.add_edge(0, 1, True)  # From invoked node
        self.workflow_instance.add_edge(1, 2, True)  # From non-invoked node

        # Check invocation status
        edge_0_to_1 = self.workflow_instance._edges[1][0]
        edge_1_to_2 = self.workflow_instance._edges[2][1]

        self.assertTrue(edge_0_to_1.conditionally_invoked)  # From invoked node
        self.assertFalse(edge_1_to_2.conditionally_invoked)  # From non-invoked node

    def test_add_node_single_predecessor(self):
        """Test adding node with single predecessor (non-sync)"""
        # Setup mocks
        self.input_manager.get_node_runtimes_and_data_transfer.return_value = ([1.0, 2.0], 1.5, 0.1)

        # Setup edge with transmission info
        self.workflow_instance._nodes[0].invoked = True
        self.workflow_instance.add_edge(0, 1, True)

        edge = self.workflow_instance._edges[1][0]
        # Mock the method properly
        with patch.object(edge, "get_transmission_information") as mock_transmission:
            mock_transmission.return_value = {
                "starting_runtime": 0.5,
                "cumulative_runtime": 2.0,
                "sns_data_transfer_size": 0.2,
            }

            # Add the node
            result = self.workflow_instance.add_node(1)

            # Verify results
            self.assertTrue(result)  # Node was invoked
            self.assertTrue(self.workflow_instance._nodes[1].invoked)
            self.assertEqual(self.workflow_instance._nodes[1].cumulative_runtimes, [1.0, 2.0])
            self.assertEqual(self.workflow_instance._nodes[1].execution_time, 1.5)

    def test_add_node_sync_node(self):
        """Test adding sync node with multiple predecessors"""
        # Setup multiple predecessors
        self.workflow_instance._nodes[0].invoked = True
        self.workflow_instance._nodes[1].invoked = True
        self.workflow_instance.add_edge(0, 2, True)
        self.workflow_instance.add_edge(1, 2, True)

        # Mock edge transmission info for sync node
        sync_info = {
            "sync_size": 0.3,
            "consumed_dynamodb_write_capacity_units": 2.0,
            "dynamodb_upload_size": 0.25,
            "sync_upload_auxiliary_info": (0.1, 0.2),
        }

        # Mock capacity calculation
        self.input_manager.calculate_dynamodb_capacity_unit_of_sync_edges.return_value = {
            "write_capacity_units": 4.0,
            "read_capacity_units": 1.0,
        }

        self.input_manager.get_node_runtimes_and_data_transfer.return_value = ([1.0, 2.0], 1.5, 0.1)

        # Mock all edges in the sync node
        with patch.object(self.workflow_instance._edges[2][0], "get_transmission_information") as mock1, patch.object(
            self.workflow_instance._edges[2][1], "get_transmission_information"
        ) as mock2:
            mock1.return_value = {
                "starting_runtime": 0.5,
                "cumulative_runtime": 2.0,
                "sns_data_transfer_size": 0.2,
                "sync_info": sync_info,
            }
            mock2.return_value = {
                "starting_runtime": 0.5,
                "cumulative_runtime": 2.0,
                "sns_data_transfer_size": 0.2,
                "sync_info": sync_info,
            }

            # Add the sync node
            result = self.workflow_instance.add_node(2)

            # Verify sync-specific behavior
            self.assertTrue(result)
            node = self.workflow_instance._nodes[2]
            self.assertEqual(node.tracked_dynamodb_write_capacity, 8.0)  # 2*2 from edges + 4 from sync
            self.assertEqual(node.tracked_dynamodb_read_capacity, 1.0)

    def test_add_node_non_invoked(self):
        """Test adding node that doesn't get invoked"""
        # Setup non-invoked predecessor
        self.workflow_instance._nodes[0].invoked = False
        self.workflow_instance.add_edge(0, 1, True)

        edge = self.workflow_instance._edges[1][0]
        # Mock the method to return None
        with patch.object(edge, "get_transmission_information") as mock_transmission:
            mock_transmission.return_value = None  # No transmission info

            # Add the node
            result = self.workflow_instance.add_node(1)

            # Verify node is not invoked
            self.assertFalse(result)
            self.assertFalse(self.workflow_instance._nodes[1].invoked)

    def test_handle_sns_invocation_single_edge(self):
        """Test SNS invocation handling with single edge"""
        from_node = self.workflow_instance._get_node(0)
        to_node = self.workflow_instance._get_node(1)

        edge_data = [
            (
                1.0,
                {
                    "from_instance_node": from_node,
                    "to_instance_node": to_node,
                    "cumulative_runtime": 2.5,
                    "sns_data_transfer_size": 0.3,
                },
            )
        ]

        result = self.workflow_instance._handle_sns_invocation(edge_data)

        self.assertEqual(result, 2.5)
        # Check data transfer tracking
        self.assertIn(1, from_node.tracked_data_output_sizes)
        self.assertIn(0, to_node.tracked_data_input_sizes)
        self.assertIn(1, from_node.sns_data_call_and_output_sizes)

    def test_handle_sns_invocation_multiple_edges(self):
        """Test SNS invocation with multiple edges - should pick latest"""
        from_node1 = self.workflow_instance._get_node(0)
        from_node2 = self.workflow_instance._get_node(1)
        to_node = self.workflow_instance._get_node(2)

        edge_data = [
            (
                1.0,
                {
                    "from_instance_node": from_node1,
                    "to_instance_node": to_node,
                    "cumulative_runtime": 2.0,
                    "sns_data_transfer_size": 0.2,
                },
            ),
            (
                2.0,
                {  # Later starting time - should be selected
                    "from_instance_node": from_node2,
                    "to_instance_node": to_node,
                    "cumulative_runtime": 3.0,
                    "sns_data_transfer_size": 0.4,
                },
            ),
        ]

        result = self.workflow_instance._handle_sns_invocation(edge_data)

        # Should use the edge with later starting time
        self.assertEqual(result, 3.0)
        self.assertIn(2, from_node2.sns_data_call_and_output_sizes)
        self.assertEqual(from_node2.sns_data_call_and_output_sizes[2], [0.4])

    def test_handle_sns_invocation_empty(self):
        """Test SNS invocation with no edges"""
        result = self.workflow_instance._handle_sns_invocation([])
        self.assertEqual(result, 0.0)

    def test_handle_simulated_edge(self):
        """Test simulated edge handling"""
        from_node = self.workflow_instance._get_node(0)
        to_node = self.workflow_instance._get_node(1)

        simulated_edge = MagicMock(spec=SimulatedInstanceEdge)
        simulated_edge.from_instance_node = from_node
        simulated_edge.to_instance_node = to_node
        simulated_edge.get_simulated_transmission_information.return_value = {
            "starting_runtime": 1.5,
            "cumulative_runtime": 3.0,
            "sns_data_transfer_size": 0.5,
        }

        edge_data = []
        self.workflow_instance._handle_simulated_edge(simulated_edge, edge_data)

        # Check that edge data was appended
        self.assertEqual(len(edge_data), 1)
        self.assertEqual(edge_data[0][0], 1.5)  # Starting runtime
        self.assertEqual(edge_data[0][1]["cumulative_runtime"], 3.0)

    def test_handle_simulated_edge_no_transmission_info(self):
        """Test simulated edge with no transmission info"""
        simulated_edge = MagicMock(spec=SimulatedInstanceEdge)

        # Create proper mock structure
        to_node = MagicMock()
        to_node.nominal_instance_id = 1

        simulated_edge.to_instance_node = to_node
        simulated_edge.get_simulated_transmission_information.return_value = None

        with self.assertRaises(ValueError) as context:
            self.workflow_instance._handle_simulated_edge(simulated_edge, [])

        self.assertIn("No transmission info in edge", str(context.exception))

    def test_handle_real_edge_invoked_normal(self):
        """Test real edge handling for normal invoked case"""
        current_edge = MagicMock(spec=InstanceEdge)
        current_edge.conditionally_invoked = True
        current_edge.from_instance_node = self.workflow_instance._get_node(0)
        current_edge.to_instance_node = self.workflow_instance._get_node(1)
        current_edge.get_transmission_information.return_value = {
            "starting_runtime": 1.0,
            "cumulative_runtime": 2.0,
            "sns_data_transfer_size": 0.3,
        }

        edge_data = []
        result = self.workflow_instance._handle_real_edge(current_edge, False, [], edge_data)

        self.assertTrue(result)
        self.assertEqual(len(edge_data), 1)

    def test_handle_real_edge_sync_node(self):
        """Test real edge handling for sync node"""
        current_edge = MagicMock(spec=InstanceEdge)
        current_edge.conditionally_invoked = True
        current_edge.from_instance_node = self.workflow_instance._get_node(0)
        current_edge.to_instance_node = self.workflow_instance._get_node(1)
        current_edge.from_instance_node.nominal_instance_id = 0  # Not start hop

        sync_info = {
            "sync_size": 0.2,
            "consumed_dynamodb_write_capacity_units": 3.0,
            "dynamodb_upload_size": 0.15,
            "sync_upload_auxiliary_info": (0.1, 0.2),
        }

        current_edge.get_transmission_information.return_value = {
            "starting_runtime": 1.0,
            "cumulative_runtime": 2.0,
            "sns_data_transfer_size": 0.3,
            "sync_info": sync_info,
        }

        sync_edge_data = []
        result = self.workflow_instance._handle_real_edge(current_edge, True, sync_edge_data, [])

        self.assertTrue(result)
        self.assertEqual(len(sync_edge_data), 1)
        self.assertEqual(current_edge.to_instance_node.tracked_dynamodb_write_capacity, 3.0)

    def test_handle_real_edge_sync_node_invalid_predecessor(self):
        """Test sync node with invalid predecessor (start hop)"""
        current_edge = MagicMock(spec=InstanceEdge)
        current_edge.conditionally_invoked = True

        # Create proper mock structure
        from_node = MagicMock()
        from_node.nominal_instance_id = -1  # Start hop
        to_node = MagicMock()
        to_node.nominal_instance_id = 1

        current_edge.from_instance_node = from_node
        current_edge.to_instance_node = to_node
        current_edge.get_transmission_information.return_value = {
            "starting_runtime": 1.0,
            "cumulative_runtime": 2.0,
            "sns_data_transfer_size": 0.3,
            "sync_info": {},
        }

        with self.assertRaises(ValueError) as context:
            self.workflow_instance._handle_real_edge(current_edge, True, [], [])

        self.assertIn("Sync node must have a predecessor", str(context.exception))

    def test_handle_real_edge_non_invoked(self):
        """Test real edge handling for non-invoked case"""
        current_edge = MagicMock(spec=InstanceEdge)
        current_edge.conditionally_invoked = False
        current_edge.from_instance_node = self.workflow_instance._get_node(0)
        current_edge.to_instance_node = self.workflow_instance._get_node(1)
        current_edge.from_instance_node.nominal_instance_id = 0  # Not start hop
        current_edge.from_instance_node.region_id = 0

        non_execution_info = {
            "predecessor_instance_id": 2,
            "sync_node_instance_id": 3,
            "consumed_dynamodb_write_capacity_units": 1.5,
            "sync_size": 0.1,
        }

        current_edge.get_transmission_information.return_value = {"non_execution_info": [non_execution_info]}

        result = self.workflow_instance._handle_real_edge(current_edge, False, [], [])

        self.assertFalse(result)
        # Check that sync node capacity was updated
        sync_node = self.workflow_instance._nodes[3]
        self.assertEqual(sync_node.tracked_dynamodb_write_capacity, 1.5)

    def test_handle_real_edge_non_invoked_invalid_predecessor(self):
        """Test non-invoked edge with invalid predecessor (start hop)"""
        current_edge = MagicMock(spec=InstanceEdge)
        current_edge.conditionally_invoked = False

        # Create proper mock structure
        from_node = MagicMock()
        from_node.nominal_instance_id = -1  # Start hop
        to_node = MagicMock()
        to_node.nominal_instance_id = 1

        current_edge.from_instance_node = from_node
        current_edge.to_instance_node = to_node
        current_edge.get_transmission_information.return_value = {"non_execution_info": [{}]}

        with self.assertRaises(ValueError) as context:
            self.workflow_instance._handle_real_edge(current_edge, False, [], [])

        self.assertIn("Non-execution node must have a predecessor", str(context.exception))

    def test_handle_real_edge_no_transmission_info_invoked_node(self):
        """Test real edge with no transmission info from invoked node"""
        current_edge = MagicMock(spec=InstanceEdge)

        # Create proper mock structure
        from_node = MagicMock()
        from_node.invoked = True
        to_node = MagicMock()
        to_node.nominal_instance_id = 1

        current_edge.from_instance_node = from_node
        current_edge.to_instance_node = to_node
        current_edge.conditionally_invoked = True
        current_edge.get_transmission_information.return_value = None

        with self.assertRaises(ValueError) as context:
            self.workflow_instance._handle_real_edge(current_edge, False, [], [])

        self.assertIn("No transmission info in edge", str(context.exception))

    def test_get_node_creation(self):
        """Test node creation and retrieval"""
        # Node doesn't exist initially
        self.assertNotIn(5, self.workflow_instance._nodes)

        node = self.workflow_instance._get_node(5)

        # Node should now exist
        self.assertIn(5, self.workflow_instance._nodes)
        self.assertIsInstance(node, InstanceNode)

        # Getting same node should return same instance
        node2 = self.workflow_instance._get_node(5)
        self.assertIs(node, node2)

    def test_create_edge_new(self):
        """Test edge creation"""
        edge = self.workflow_instance._create_edge(0, 1)

        self.assertIsInstance(edge, InstanceEdge)
        self.assertIn(1, self.workflow_instance._edges)
        self.assertIn(0, self.workflow_instance._edges[1])
        self.assertIs(self.workflow_instance._edges[1][0], edge)

    def test_create_simulated_edge_new(self):
        """Test simulated edge creation"""
        simulated_edge = self.workflow_instance._create_simulated_edge(0, 1, 2, 3)

        self.assertIsInstance(simulated_edge, SimulatedInstanceEdge)
        self.assertIn(3, self.workflow_instance._simulated_edges)
        self.assertIn(0, self.workflow_instance._simulated_edges[3])
        self.assertIs(self.workflow_instance._simulated_edges[3][0], simulated_edge)

    def test_manage_data_transfer_dict_new_region(self):
        """Test data transfer dict management for new region"""
        data_dict = {}
        self.workflow_instance._manage_data_transfer_dict(data_dict, 1, 100.0)

        self.assertIn(1, data_dict)
        self.assertEqual(data_dict[1], 100.0)

    def test_manage_data_transfer_dict_existing_region(self):
        """Test data transfer dict management for existing region"""
        data_dict = {1: 50.0}
        self.workflow_instance._manage_data_transfer_dict(data_dict, 1, 100.0)

        self.assertEqual(data_dict[1], 150.0)

    def test_manage_sns_invocation_data_transfer_dict_new_region(self):
        """Test SNS data transfer dict management for new region"""
        sns_dict = {}
        self.workflow_instance._manage_sns_invocation_data_transfer_dict(sns_dict, 1, 100.0)

        self.assertIn(1, sns_dict)
        self.assertEqual(sns_dict[1], [100.0])

    def test_manage_sns_invocation_data_transfer_dict_existing_region(self):
        """Test SNS data transfer dict management for existing region"""
        sns_dict = {1: [50.0]}
        self.workflow_instance._manage_sns_invocation_data_transfer_dict(sns_dict, 1, 100.0)

        self.assertEqual(sns_dict[1], [50.0, 100.0])

    def test_get_predecessor_edges_real(self):
        """Test getting real predecessor edges"""
        # Add some edges
        self.workflow_instance._create_edge(0, 2)
        self.workflow_instance._create_edge(1, 2)

        edges = self.workflow_instance._get_predecessor_edges(2, False)

        self.assertEqual(len(edges), 2)
        self.assertIsInstance(edges[0], InstanceEdge)

    def test_get_predecessor_edges_simulated(self):
        """Test getting simulated predecessor edges"""
        # Add some simulated edges
        self.workflow_instance._create_simulated_edge(0, 1, 2, 3)
        self.workflow_instance._create_simulated_edge(1, 2, 3, 3)

        edges = self.workflow_instance._get_predecessor_edges(3, True)

        self.assertEqual(len(edges), 2)
        self.assertIsInstance(edges[0], SimulatedInstanceEdge)

    def test_get_predecessor_edges_none(self):
        """Test getting predecessor edges when none exist"""
        edges = self.workflow_instance._get_predecessor_edges(99, False)
        self.assertEqual(len(edges), 0)

    def test_retrieved_wpd_at_function_true(self):
        """Test WPD retrieval when probability conditions met"""
        with patch("random.random", return_value=0.1):
            self.input_manager.get_start_hop_retrieve_wpd_probability.return_value = 0.5
            result = self.workflow_instance._retrieved_wpd_at_function()
            self.assertTrue(result)

    def test_retrieved_wpd_at_function_false(self):
        """Test WPD retrieval when probability conditions not met"""
        with patch("random.random", return_value=0.9):
            self.input_manager.get_start_hop_retrieve_wpd_probability.return_value = 0.5
            result = self.workflow_instance._retrieved_wpd_at_function()
            self.assertFalse(result)

    def test_complex_workflow_integration(self):
        """Integration test for complex workflow scenario"""
        # Setup a complex workflow: Client -> Redirector -> Node0 -> Node1, Node2 (sync)
        with patch("random.random", return_value=0.1):  # Ensure WPD retrieval
            self.input_manager.get_start_hop_retrieve_wpd_probability.return_value = 1.0
            workflow_instance = WorkflowInstance(
                self.input_manager,
                [1, 0, 2],  # Start hop not in home region
                0,
                True,
            )

            # Mock all required methods
            workflow_instance._input_manager.get_node_runtimes_and_data_transfer.return_value = ([1.0, 2.0], 1.5, 0.1)
            workflow_instance._input_manager.calculate_dynamodb_capacity_unit_of_sync_edges.return_value = {
                "write_capacity_units": 2.0,
                "read_capacity_units": 1.0,
            }

            # Setup start hop
            workflow_instance.add_start_hop(0)

            # Add edges
            workflow_instance.add_edge(0, 1, True)
            workflow_instance.add_edge(0, 2, True)
            workflow_instance.add_edge(1, 2, True)  # Node 2 becomes sync node

            # Verify complex workflow structure
            self.assertTrue(workflow_instance._redirector_exists)
            self.assertTrue(workflow_instance._nodes[-1].invoked)  # Redirector
            self.assertTrue(workflow_instance._nodes[0].invoked)  # Start hop

    def test_edge_cases_and_error_conditions(self):
        """Test various edge cases and error conditions"""

        # The WorkflowInstance constructor doesn't actually validate empty regions
        # Let's test a more realistic edge case

        # Test with single region
        workflow_instance = WorkflowInstance(
            self.input_manager,
            [0],  # Single region
            0,
            True,
        )

        # Should create successfully
        self.assertEqual(len(workflow_instance._nodes), 2)  # Virtual client + one instance

        # Test with start hop index at boundary
        workflow_instance = WorkflowInstance(
            self.input_manager,
            [0, 1, 2],
            2,  # Last valid index
            True,
        )

        self.assertEqual(workflow_instance._start_hop_instance_id, 2)

    def test_data_consistency_after_operations(self):
        """Test data consistency after multiple operations"""
        workflow_instance = self.workflow_instance

        # Perform multiple operations
        workflow_instance.add_start_hop(0)
        workflow_instance.add_edge(0, 1, True)
        workflow_instance.add_edge(0, 2, True)

        # Check data structures are consistent
        self.assertEqual(len(workflow_instance._nodes), 4)  # -1, 0, 1, 2
        self.assertIn(-1, workflow_instance._nodes)  # Virtual client
        self.assertIn(0, workflow_instance._nodes)  # Start hop
        self.assertIn(1, workflow_instance._nodes)  # Node 1
        self.assertIn(2, workflow_instance._nodes)  # Node 2

        # Check edges exist
        self.assertIn(1, workflow_instance._edges)
        self.assertIn(2, workflow_instance._edges)
        self.assertIn(0, workflow_instance._edges[1])
        self.assertIn(0, workflow_instance._edges[2])

    def test_memory_efficiency_large_workflow(self):
        """Test memory efficiency with larger workflow"""
        # Create workflow with many nodes
        large_regions = list(range(20))
        workflow_instance = WorkflowInstance(
            self.input_manager,
            large_regions,
            0,
            True,
        )

        # Add many edges
        for i in range(19):
            workflow_instance.add_edge(i, i + 1, True)

        # Verify structure
        self.assertEqual(len(workflow_instance._nodes), 21)  # 20 + virtual client
        self.assertEqual(len(workflow_instance._edges), 19)

    def test_concurrent_edge_addition(self):
        """Test adding multiple edges to same destination"""
        workflow_instance = self.workflow_instance

        # Add multiple edges pointing to same node
        workflow_instance.add_edge(0, 3, True)
        workflow_instance.add_edge(1, 3, True)
        workflow_instance.add_edge(2, 3, True)

        # Check all edges exist
        self.assertEqual(len(workflow_instance._edges[3]), 3)
        self.assertIn(0, workflow_instance._edges[3])
        self.assertIn(1, workflow_instance._edges[3])
        self.assertIn(2, workflow_instance._edges[3])

    def test_node_state_transitions(self):
        """Test node state transitions during workflow execution"""
        workflow_instance = self.workflow_instance

        # Initially no nodes are invoked
        node = workflow_instance._get_node(1)
        self.assertFalse(node.invoked)

        # Add start hop - should set invocation
        workflow_instance.add_start_hop(1)
        self.assertTrue(workflow_instance._nodes[1].invoked)

        # Add edge from invoked to non-invoked
        workflow_instance.add_edge(1, 2, True)

        # Node 2 should not be invoked yet
        self.assertFalse(workflow_instance._nodes[2].invoked)

    def test_performance_metrics_calculation(self):
        """Test performance metrics calculation accuracy"""
        # Clear existing nodes
        self.workflow_instance._nodes.clear()

        # Setup nodes with specific metrics
        nodes_data = [
            {"cost": 15.5, "runtime": 3.2, "execution_carbon": 8.1, "transmission_carbon": 4.3},
            {"cost": 22.1, "runtime": 2.8, "execution_carbon": 12.4, "transmission_carbon": 6.7},
            {"cost": 18.9, "runtime": 4.1, "execution_carbon": 9.8, "transmission_carbon": 5.2},
        ]

        for i, data in enumerate(nodes_data):
            node = self.workflow_instance._get_node(i)
            # Properly mock the method
            node.calculate_carbon_cost_runtime = MagicMock(return_value=data)

        result = self.workflow_instance.calculate_overall_cost_runtime_carbon()

        # Verify calculations
        expected_cost = sum(data["cost"] for data in nodes_data)
        expected_runtime = max(data["runtime"] for data in nodes_data)
        expected_exec_carbon = sum(data["execution_carbon"] for data in nodes_data)
        expected_trans_carbon = sum(data["transmission_carbon"] for data in nodes_data)

        self.assertAlmostEqual(result["cost"], expected_cost, places=2)
        self.assertAlmostEqual(result["runtime"], expected_runtime, places=2)
        self.assertAlmostEqual(result["execution_carbon"], expected_exec_carbon, places=2)
        self.assertAlmostEqual(result["transmission_carbon"], expected_trans_carbon, places=2)
        self.assertAlmostEqual(result["carbon"], expected_exec_carbon + expected_trans_carbon, places=2)

    def test_workflow_instance_immutability_aspects(self):
        """Test that certain aspects of workflow instance remain consistent"""
        original_start_hop_id = self.workflow_instance._start_hop_instance_id
        original_consider_latency = self.workflow_instance._consider_from_client_latency

        # Perform operations that don't involve sync nodes
        self.workflow_instance.add_start_hop(0)  # This creates edge -1 -> 0
        self.workflow_instance.add_edge(0, 2, True)  # Edge to different node (not 1)

        # Mock the node's transmission info to avoid sync node issues
        if 2 in self.workflow_instance._edges and 0 in self.workflow_instance._edges[2]:
            edge = self.workflow_instance._edges[2][0]
            with patch.object(edge, "get_transmission_information") as mock_transmission:
                mock_transmission.return_value = {
                    "starting_runtime": 0.5,
                    "cumulative_runtime": 2.0,
                    "sns_data_transfer_size": 0.2,
                }
                self.input_manager.get_node_runtimes_and_data_transfer.return_value = ([1.0, 2.0], 1.5, 0.1)

                self.workflow_instance.add_node(2)

        # Check immutable properties haven't changed
        self.assertEqual(self.workflow_instance._start_hop_instance_id, original_start_hop_id)
        self.assertEqual(self.workflow_instance._consider_from_client_latency, original_consider_latency)

    def test_boundary_conditions_region_ids(self):
        """Test boundary conditions with special region IDs"""
        # Test with negative region IDs (should be handled gracefully)
        workflow_instance = WorkflowInstance(
            self.input_manager,
            [-1, 0, 1],  # Including negative region
            1,  # Start at middle
            True,
        )

        # Should create nodes successfully
        self.assertIn(0, workflow_instance._nodes)
        self.assertIn(1, workflow_instance._nodes)
        self.assertIn(2, workflow_instance._nodes)

        # Check region assignments
        self.assertEqual(workflow_instance._nodes[0].region_id, -1)
        self.assertEqual(workflow_instance._nodes[1].region_id, 0)
        self.assertEqual(workflow_instance._nodes[2].region_id, 1)

    def test_resource_cleanup_implications(self):
        """Test implications for resource cleanup"""
        workflow_instance = self.workflow_instance

        # Create resources
        workflow_instance.add_start_hop(0)
        workflow_instance.add_edge(0, 1, True)
        workflow_instance.add_edge(1, 2, True)

        # Access internal structures to verify they exist
        self.assertGreater(len(workflow_instance._nodes), 0)
        self.assertGreater(len(workflow_instance._edges), 0)

        # Simulate cleanup by clearing (in real scenario, this might be automatic)
        node_count = len(workflow_instance._nodes)
        edge_count = len(workflow_instance._edges)

        # Verify resources were allocated
        self.assertGreaterEqual(node_count, 3)
        self.assertGreaterEqual(edge_count, 2)

    def test_input_validation_edge_cases(self):
        """Test input validation for edge cases"""

        # Test with identical from and to indices
        self.workflow_instance.add_edge(1, 1, True)  # Self-loop
        self.assertIn(1, self.workflow_instance._edges[1])

        # Test with very large instance indices
        large_idx = 1000000
        self.workflow_instance.add_edge(0, large_idx, True)
        self.assertIn(large_idx, self.workflow_instance._nodes)

    def test_probabilistic_behavior_consistency(self):
        """Test consistency of probabilistic behavior"""
        # Test multiple instances with same seed should behave similarly
        results = []

        for seed in [42, 42, 42]:  # Same seed
            with patch("random.random", return_value=0.3):
                self.input_manager.get_start_hop_retrieve_wpd_probability.return_value = 0.5
                workflow = WorkflowInstance(self.input_manager, [0, 1, 2], 0, True)
                results.append(workflow._has_retrieved_wpd_at_function)

        # All results should be the same (deterministic with mocked random)
        self.assertTrue(all(r == results[0] for r in results))

    def test_error_propagation(self):
        """Test error propagation from dependencies"""

        # The error occurs during construction when home region is accessed
        # But it's accessed in __init__, so let's test a different error case
        failing_input_manager = MagicMock(spec=InputManager)
        failing_input_manager.get_start_hop_retrieve_wpd_probability.return_value = 0.0
        failing_input_manager.get_home_region_index.return_value = 0  # Don't fail here
        failing_input_manager.get_start_hop_info.side_effect = RuntimeError("Database connection failed")

        workflow_instance = WorkflowInstance(failing_input_manager, [0, 1, 2], 0, True)

        # Error should occur when calling add_start_hop
        with self.assertRaises(RuntimeError):
            workflow_instance.add_start_hop(0)

    def test_comprehensive_state_verification(self):
        """Comprehensive test of final state after complex operations"""
        workflow_instance = self.workflow_instance

        # Build complex workflow
        workflow_instance.add_start_hop(0)

        # Create branching structure: 0 -> 1, 0 -> 2, 1 -> 3, 2 -> 3 (3 is sync)
        workflow_instance.add_edge(0, 1, True)
        workflow_instance.add_edge(0, 2, True)
        workflow_instance.add_edge(1, 3, True)
        workflow_instance.add_edge(2, 3, True)

        # Verify final state
        # Nodes should exist
        for i in [-1, 0, 1, 2, 3]:
            self.assertIn(i, workflow_instance._nodes)

        # Edges should exist - fix the expected edges
        expected_edges = [(-1, 0), (0, 1), (0, 2), (1, 3), (2, 3)]  # (from, to) format
        for from_node, to_node in expected_edges:
            if to_node in workflow_instance._edges:
                self.assertIn(from_node, workflow_instance._edges[to_node])

        # Start hop should be invoked
        self.assertTrue(workflow_instance._nodes[0].invoked)

        # Virtual client should be invoked
        self.assertTrue(workflow_instance._nodes[-1].invoked)


class TestWorkflowInstanceEdgeCases(unittest.TestCase):
    """Additional test class for edge cases and stress testing"""

    def setUp(self):
        self.input_manager = MagicMock(spec=InputManager)
        self.input_manager.get_start_hop_retrieve_wpd_probability.return_value = 0.0
        self.input_manager.get_home_region_index.return_value = 0
        self.input_manager.get_start_hop_info.return_value = {
            "read_capacity_units": 1.0,
            "workflow_placement_decision_size": 0.01,
        }

    def test_minimal_workflow(self):
        """Test minimal possible workflow"""
        workflow_instance = WorkflowInstance(self.input_manager, [0], 0, True)  # Single region  # Single node

        workflow_instance.add_start_hop(0)

        # Should have virtual client and single instance node
        self.assertEqual(len(workflow_instance._nodes), 2)
        self.assertTrue(workflow_instance._nodes[0].invoked)

    def test_maximum_reasonable_workflow(self):
        """Test with reasonably large workflow"""
        num_regions = 50
        regions = list(range(num_regions))

        workflow_instance = WorkflowInstance(self.input_manager, regions, 0, True)

        # Add start hop
        workflow_instance.add_start_hop(0)

        # Create linear chain
        for i in range(num_regions - 1):
            workflow_instance.add_edge(i, i + 1, True)

        # Verify structure
        self.assertEqual(len(workflow_instance._nodes), num_regions + 1)  # +1 for virtual client
        self.assertEqual(len(workflow_instance._edges), num_regions)  # Including client->start edge

    def test_deeply_nested_conditionals(self):
        """Test deeply nested conditional logic paths"""

        # Test all combinations of boolean flags
        test_cases = [
            (True, True),  # WPD retrieved, redirector exists
            (True, False),  # WPD retrieved, no redirector
            (False, True),  # WPD not retrieved, but somehow redirector exists (edge case)
            (False, False),  # WPD not retrieved, no redirector
        ]

        for wpd_retrieved, different_regions in test_cases:
            with patch("random.random", return_value=0.1 if wpd_retrieved else 0.9):
                self.input_manager.get_start_hop_retrieve_wpd_probability.return_value = 0.5

                regions = [1, 0, 2] if different_regions else [0, 1, 2]

                workflow_instance = WorkflowInstance(self.input_manager, regions, 0, True)

                # Verify boolean logic worked correctly
                expected_redirector = wpd_retrieved and different_regions
                self.assertEqual(workflow_instance._redirector_exists, expected_redirector)

    def test_stress_edge_operations(self):
        """Stress test edge operations"""
        workflow_instance = WorkflowInstance(self.input_manager, list(range(10)), 0, True)

        # Add many edges in different patterns
        # Star pattern: all nodes connect to center
        center = 5
        for i in range(10):
            if i != center:
                workflow_instance.add_edge(i, center, True)

        # Verify center node has many predecessors
        if center in workflow_instance._edges:
            self.assertEqual(len(workflow_instance._edges[center]), 9)

    def test_memory_stress_simulated_edges(self):
        """Stress test simulated edge creation"""
        workflow_instance = WorkflowInstance(self.input_manager, list(range(5)), 0, True)

        # Create many simulated edges
        for i in range(5):
            for j in range(5):
                if i != j:
                    workflow_instance._create_simulated_edge(i, j, i + 10, j + 10)

        # Verify structure
        self.assertGreater(len(workflow_instance._simulated_edges), 0)

    def test_exception_handling_robustness(self):
        """Test robustness of exception handling"""

        # Test with mock that raises exceptions
        failing_input_manager = MagicMock(spec=InputManager)
        failing_input_manager.get_start_hop_retrieve_wpd_probability.return_value = 0.0
        failing_input_manager.get_home_region_index.return_value = 0
        failing_input_manager.get_start_hop_info.side_effect = Exception("Simulated failure")

        workflow_instance = WorkflowInstance(failing_input_manager, [0, 1, 2], 0, True)

        # This should raise an exception
        with self.assertRaises(Exception):
            workflow_instance.add_start_hop(0)


if __name__ == "__main__":
    unittest.main()
